// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package scene

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

type SchedulingSceneManager struct {
	ctx     context.Context
	scene   atomic.Value
	opt     *config.PersistOptions
	storage *core.Storage
}

func NewSchedulingSceneManager(ctx context.Context, opt *config.PersistOptions, storage *core.Storage) *SchedulingSceneManager {
	m := &SchedulingSceneManager{
		ctx:     ctx,
		opt:     opt,
		storage: storage,
	}
	m.scene.Store(def)
	go m.checkScene()
	return m
}

func (m *SchedulingSceneManager) checkScene() {
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			time.Sleep(5 * time.Second)
		}
		scene := stringToSchedulingScene(m.opt.GetSchedulingScene())
		if m.scene.Load() != scene {
			err := m.saveCurrentSettings()
			err1 := m.loadPrevSettings(scene)
			// TODO: support some other actions
			if err == nil && err1 == nil {
				m.scene.Store(scene)
			}
		}
	}
}

func (m *SchedulingSceneManager) saveCurrentSettings() error {
	cfg := m.opt.GetScheduleConfig().Clone()
	scene := m.scene.Load().(schedulingScene).String()
	if err := m.storage.SaveScene(scene, cfg); err != nil {
		log.Error("cannot save current settings", zap.String("scene", scene), errs.ZapError(err))
		return err
	}
	return nil
}

func (m *SchedulingSceneManager) loadPrevSettings(scene schedulingScene) error {
	// load settings according to the given scene.
	cfg := &config.ScheduleConfig{}
	isExist, err := m.storage.LoadScene(scene.String(), cfg)
	if err != nil {
		log.Error("cannot load previous settings", zap.String("scene", scene.String()), errs.ZapError(err))
		return err
	}

	// if we have a previous settings for this scene
	if isExist {
		// update the current config
		m.opt.SetScheduleConfig(cfg)
		if err := m.opt.Persist(m.storage); err != nil {
			log.Error("cannot persist schedule config", errs.ZapError(err))
			return err
		}
		return nil
	}
	// use the default settings
	return m.applyDefaultSettings(scene)
}

func (m *SchedulingSceneManager) applyDefaultSettings(scene schedulingScene) error {
	cfg := m.opt.GetScheduleConfig().Clone()
	switch scene {
	case online:
		// TODO: support custom configuration for online mode.
	case offline:
		cfg.ReplicaScheduleLimit = 128
		cfg.PatrolRegionInterval.Duration = time.Millisecond
	case repair:
		cfg.ReplicaScheduleLimit = 128
		cfg.PatrolRegionInterval.Duration = time.Millisecond
	}
	m.opt.SetScheduleConfig(cfg)
	if err := m.opt.Persist(m.storage); err != nil {
		log.Error("cannot persist schedule config", errs.ZapError(err))
		return err
	}
	return nil
}

func (m *SchedulingSceneManager) GetCurrentScene() string {
	return m.scene.Load().(schedulingScene).String()
}
