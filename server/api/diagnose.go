package api

import (
	"net/http"

	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type diagnosisHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newDiagnosisHandler(svr *server.Server, rd *render.Render) *diagnosisHandler {
	return &diagnosisHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *diagnosisHandler) GetSchedulerState(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	name := r.URL.Query().Get("name")
	s, err := rc.GetSchedulerState(name)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}

	h.rd.JSON(w, http.StatusOK, s)

}
