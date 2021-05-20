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

type schedulingScene int

const (
	def schedulingScene = iota
	online
	offline
	repair
)

func (s schedulingScene) String() string {
	switch s {
	case def:
		return "def"
	case online:
		return "online"
	case offline:
		return "offline"
	case repair:
		return "repair"
	default:
		return "unknown"
	}
}

// stringToSchedulingScene creates a scheduling scene with string.
func stringToSchedulingScene(input string) schedulingScene {
	switch input {
	case def.String():
		return def
	case online.String():
		return online
	case offline.String():
		return offline
	case repair.String():
		return repair
	default:
		panic("invalid scheduling scene: " + input)
	}
}
