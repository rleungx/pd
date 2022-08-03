package api

import (
	"net/http"
	"time"

	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type diagHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newDiagHandler(svr *server.Server, rd *render.Render) *diagHandler {
	return &diagHandler{
		svr: svr,
		rd:  rd,
	}
}

type Diag struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Summary   string `json:"summary"`
	Timestamp int64  `json:"timestamp"`
}

func (h *diagHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	status, summary, err := rc.GetCoordinator().GetStatus()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}

	d := &Diag{
		Name:      "balance-region-scheduler",
		Status:    status,
		Summary:   summary,
		Timestamp: time.Now().Unix(),
	}

	h.rd.JSON(w, http.StatusOK, d)
}
