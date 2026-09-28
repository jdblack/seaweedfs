package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// ECConfigHandlers serves the EC ratio policy page and its API. The page is the
// dashboard half of `ec.config`: the same document in the filer, edited here.
type ECConfigHandlers struct {
	adminServer *dash.AdminServer
}

func NewECConfigHandlers(adminServer *dash.AdminServer) *ECConfigHandlers {
	return &ECConfigHandlers{adminServer: adminServer}
}

// ecConfigSetRequest is the payload of the set endpoints, matching the field
// names the enterprise edition's page posts.
type ecConfigSetRequest struct {
	Collection   string `json:"collection"`
	DataShards   int    `json:"dataShards"`
	ParityShards int    `json:"parityShards"`
}

// decodeECConfigSetRequest parses and validates one set request. Validation
// reuses the store's own bounds, so a policy that could never be encoded cannot
// reach the filer through the UI either.
func decodeECConfigSetRequest(body io.Reader, requireCollection bool) (ecConfigSetRequest, error) {
	var request ecConfigSetRequest
	decoder := json.NewDecoder(body)
	if err := decoder.Decode(&request); err != nil {
		return request, fmt.Errorf("invalid JSON body: %w", err)
	}
	request.Collection = strings.TrimSpace(request.Collection)
	if requireCollection && request.Collection == "" {
		return request, fmt.Errorf("collection is required")
	}
	entry := erasure_coding.ECConfigEntry{DataShards: request.DataShards, ParityShards: request.ParityShards}
	if err := entry.Validate(); err != nil {
		return request, err
	}
	return request, nil
}

// ShowECConfig renders the page, refreshing the in-process policy from the filer
// first so the page shows what the cluster actually has (an operator may have
// changed it with `ec.config` from a shell).
func (h *ECConfigHandlers) ShowECConfig(w http.ResponseWriter, r *http.Request) {
	if err := h.adminServer.ReloadECPolicyFromFiler(); err != nil {
		glog.V(1).Infof("EC Configuration: could not reload the policy from the filer: %v", err)
	}

	username := usernameOrDefault(r)
	w.Header().Set("Content-Type", "text/html")
	viewCtx := layout.NewViewContext(r, username, dash.CSRFTokenFromContext(r.Context()))
	view := layout.Layout(viewCtx, app.ECConfigPage(h.adminServer.ECConfigView()))
	if err := view.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
	}
}

// GetECConfigAPI returns the policy the admin currently holds.
func (h *ECConfigHandlers) GetECConfigAPI(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.adminServer.ECConfigView())
}

// SetGlobalECConfigAPI sets or replaces the global default.
func (h *ECConfigHandlers) SetGlobalECConfigAPI(w http.ResponseWriter, r *http.Request) {
	request, err := decodeECConfigSetRequest(r.Body, false)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := erasure_coding.SetGlobalECConfig(request.DataShards, request.ParityShards); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.persistAndRespond(w, r)
}

// DeleteGlobalECConfigAPI clears the global default: with no global and no
// override, new volumes fall back to the build default.
func (h *ECConfigHandlers) DeleteGlobalECConfigAPI(w http.ResponseWriter, r *http.Request) {
	erasure_coding.DeleteGlobalECConfig()
	h.persistAndRespond(w, r)
}

// SetCollectionECConfigAPI sets or replaces one collection's override.
func (h *ECConfigHandlers) SetCollectionECConfigAPI(w http.ResponseWriter, r *http.Request) {
	request, err := decodeECConfigSetRequest(r.Body, true)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := erasure_coding.SetCollectionECConfig(request.Collection, request.DataShards, request.ParityShards); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.persistAndRespond(w, r)
}

// DeleteCollectionECConfigAPI removes a collection's override, so it falls back
// to the global default.
func (h *ECConfigHandlers) DeleteCollectionECConfigAPI(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Collection string `json:"collection"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	if err := erasure_coding.DeleteCollectionECConfig(strings.TrimSpace(request.Collection)); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.persistAndRespond(w, r)
}

// persistAndRespond writes the policy to the filer and answers with the state
// that is now stored. The write is what makes the change cluster-wide; a failure
// is reported instead of pretending the edit took effect, and the in-memory
// change is rolled back from the filer's copy so the page never shows a policy
// the cluster does not have.
func (h *ECConfigHandlers) persistAndRespond(w http.ResponseWriter, r *http.Request) {
	if err := h.adminServer.SaveECPolicyToFiler(); err != nil {
		if reloadErr := h.adminServer.ReloadECPolicyFromFiler(); reloadErr != nil {
			glog.Warningf("EC Configuration: save failed (%v) and reload failed (%v): this process may hold an unsaved policy", err, reloadErr)
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, h.adminServer.ECConfigView())
}
