package users

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

const (
	RoleAdmin        = "admin"
	RoleViewer       = "viewer"
	RoleAlertManager = "alert-manager"
	RoleUser         = "user"
	RoleReadOnly     = "readonly"
)

var validRoles = map[string]bool{
	RoleAdmin:        true,
	RoleViewer:       true,
	RoleAlertManager: true,
	RoleUser:         true,
	RoleReadOnly:     true,
}

type RoleManager struct {
	app core.App
}

func NewRoleManager(app core.App) *RoleManager {
	return &RoleManager{app: app}
}

func (rm *RoleManager) IsAdmin(userID string) bool {
	user, err := rm.app.FindRecordById("users", userID)
	if err != nil {
		return false
	}
	return user.GetString("role") == RoleAdmin
}

func (rm *RoleManager) HasRole(userID, requiredRole string) bool {
	user, err := rm.app.FindRecordById("users", userID)
	if err != nil {
		return false
	}
	role := user.GetString("role")

	switch requiredRole {
	case RoleAdmin:
		return role == RoleAdmin
	case RoleAlertManager:
		return role == RoleAdmin || role == RoleAlertManager
	case RoleViewer:
		return role == RoleAdmin || role == RoleViewer || role == RoleAlertManager || role == RoleUser
	default:
		return true
	}
}

func (rm *RoleManager) GetSystemRole(userID, systemID string) string {
	user, err := rm.app.FindRecordById("users", userID)
	if err != nil {
		return ""
	}
	baseRole := user.GetString("role")

	record, err := rm.app.FindFirstRecordByFilter(
		"system_user_roles",
		"user={:user} AND system={:system}",
		dbx.Params{"user": userID, "system": systemID},
	)
	if err != nil {
		return baseRole
	}

	overrideRole := record.GetString("role")
	if overrideRole != "" && validRoles[overrideRole] {
		return overrideRole
	}
	return baseRole
}

func (rm *RoleManager) SetSystemRole(userID, systemID, role string) error {
	if !validRoles[role] {
		return &httpError{status: http.StatusBadRequest, message: "Invalid role"}
	}

	record, err := rm.app.FindFirstRecordByFilter(
		"system_user_roles",
		"user={:user} AND system={:system}",
		dbx.Params{"user": userID, "system": systemID},
	)
	if err != nil {
		collection, err := rm.app.FindCollectionByNameOrId("system_user_roles")
		if err != nil {
			return err
		}
		record = core.NewRecord(collection)
		record.Set("user", userID)
		record.Set("system", systemID)
	}

	record.Set("role", role)
	return rm.app.Save(record)
}

func (rm *RoleManager) GetUserAPIKeyRole(userID, token string) string {
	record, err := rm.app.FindFirstRecordByFilter(
		"api_keys",
		"user={:user} AND token={:token}",
		dbx.Params{"user": userID, "token": token},
	)
	if err != nil {
		return ""
	}
	return record.GetString("role")
}

func (rm *RoleManager) CreateAPIKey(userID, name, role string) (string, error) {
	if role != "" && !validRoles[role] {
		return "", &httpError{status: http.StatusBadRequest, message: "Invalid role"}
	}

	collection, err := rm.app.FindCollectionByNameOrId("api_keys")
	if err != nil {
		return "", err
	}

	token := generateToken()
	record := core.NewRecord(collection)
	record.Set("user", userID)
	record.Set("name", name)
	record.Set("token", token)
	record.Set("role", role)

	if err := rm.app.Save(record); err != nil {
		return "", err
	}

	return token, nil
}

func generateToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return "bsk_" + hex.EncodeToString(b)
}

type httpError struct {
	status  int
	message string
}

func (e *httpError) Error() string {
	return e.message
}
