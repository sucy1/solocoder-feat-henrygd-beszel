package alerts

import (
	"encoding/json"
	"net/http"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

type DashboardWidget struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	X        int                    `json:"x"`
	Y        int                    `json:"y"`
	Width    int                    `json:"w"`
	Height   int                    `json:"h"`
	Config   map[string]interface{} `json:"config,omitempty"`
}

type DashboardLayout struct {
	Widgets   []DashboardWidget `json:"widgets"`
	Version   int               `json:"version"`
	UpdatedAt string            `json:"updatedAt,omitempty"`
}

var defaultDashboardLayout = DashboardLayout{
	Widgets: []DashboardWidget{
		{ID: "cpu", Type: "cpu", X: 0, Y: 0, Width: 1, Height: 1},
		{ID: "memory", Type: "memory", X: 1, Y: 0, Width: 1, Height: 1},
		{ID: "disk", Type: "disk", X: 0, Y: 1, Width: 1, Height: 1},
		{ID: "network", Type: "network", X: 1, Y: 1, Width: 1, Height: 1},
		{ID: "load", Type: "load", X: 0, Y: 2, Width: 1, Height: 1},
		{ID: "temperature", Type: "temperature", X: 1, Y: 2, Width: 1, Height: 1},
	},
	Version: 1,
}

func (am *AlertManager) GetDashboardLayout(e *core.RequestEvent) error {
	userID := e.Auth.Id
	systemID := e.Request.URL.Query().Get("system")

	layout, err := am.getUserDashboardLayout(userID, systemID)
	if err != nil {
		return e.InternalServerError("", err)
	}

	return e.JSON(http.StatusOK, layout)
}

func (am *AlertManager) SaveDashboardLayout(e *core.RequestEvent) error {
	userID := e.Auth.Id
	systemID := e.Request.URL.Query().Get("system")

	var layout DashboardLayout
	if err := e.BindBody(&layout); err != nil {
		return e.BadRequestError("Invalid layout data", err)
	}

	if err := am.saveUserDashboardLayout(userID, systemID, &layout); err != nil {
		return e.InternalServerError("", err)
	}

	return e.JSON(http.StatusOK, map[string]bool{"success": true})
}

func (am *AlertManager) ResetDashboardLayout(e *core.RequestEvent) error {
	userID := e.Auth.Id
	systemID := e.Request.URL.Query().Get("system")

	if err := am.resetUserDashboardLayout(userID, systemID); err != nil {
		return e.InternalServerError("", err)
	}

	return e.JSON(http.StatusOK, defaultDashboardLayout)
}

func (am *AlertManager) getUserDashboardLayout(userID, systemID string) (*DashboardLayout, error) {
	record, err := am.hub.FindFirstRecordByFilter(
		"user_settings", "user={:user}",
		dbx.Params{"user": userID},
	)
	if err != nil {
		return &defaultDashboardLayout, nil
	}

	var settings struct {
		Dashboards map[string]DashboardLayout `json:"dashboards"`
	}
	if err := record.UnmarshalJSONField("settings", &settings); err != nil {
		return &defaultDashboardLayout, nil
	}

	key := "default"
	if systemID != "" {
		key = systemID
	}

	if layout, ok := settings.Dashboards[key]; ok {
		return &layout, nil
	}

	return &defaultDashboardLayout, nil
}

func (am *AlertManager) saveUserDashboardLayout(userID, systemID string, layout *DashboardLayout) error {
	record, err := am.hub.FindFirstRecordByFilter(
		"user_settings", "user={:user}",
		dbx.Params{"user": userID},
	)
	if err != nil {
		collection, err := am.hub.FindCollectionByNameOrId("user_settings")
		if err != nil {
			return err
		}
		record = core.NewRecord(collection)
		record.Set("user", userID)
	}

	var settings map[string]interface{}
	if err := record.UnmarshalJSONField("settings", &settings); err != nil {
		settings = make(map[string]interface{})
	}

	key := "default"
	if systemID != "" {
		key = systemID
	}

	dashboards := make(map[string]interface{})
	if d, ok := settings["dashboards"]; ok {
		if dmap, ok := d.(map[string]interface{}); ok {
			dashboards = dmap
		}
	}

	layoutJSON, err := json.Marshal(layout)
	if err != nil {
		return err
	}
	var layoutMap map[string]interface{}
	if err := json.Unmarshal(layoutJSON, &layoutMap); err != nil {
		return err
	}
	dashboards[key] = layoutMap
	settings["dashboards"] = dashboards

	record.Set("settings", settings)
	return am.hub.Save(record)
}

func (am *AlertManager) resetUserDashboardLayout(userID, systemID string) error {
	return am.saveUserDashboardLayout(userID, systemID, &defaultDashboardLayout)
}
