package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestAdminRequiresBasicAuth(t *testing.T) {
	handler := newTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if response.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("WWW-Authenticate header is missing")
	}
}

func TestTenantCRUDRoutes(t *testing.T) {
	handler := newTestHandler(t)
	value := testTenant()

	response := serveJSON(t, handler, http.MethodPost, "/api/v1/tenants", value)
	if response.Code != http.StatusOK {
		t.Fatalf("POST tenant status = %d, body = %s", response.Code, response.Body.String())
	}

	request := authorizedRequest(t, http.MethodGet, "/api/v1/tenants/tenant-a", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET tenant status = %d, body = %s", response.Code, response.Body.String())
	}
	var got tenant.Tenant
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode tenant response: %v", err)
	}
	if got.ID != value.ID {
		t.Fatalf("tenant ID = %q, want %q", got.ID, value.ID)
	}
}

func TestAppBindingAndActivationRoutes(t *testing.T) {
	store := newMemoryStore()
	cache := &recordingCache{}
	handler, err := NewHandler(store, cache, "admin", "test-password")
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	if err := store.UpsertTenant(context.Background(), testTenant()); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	app := testApp()
	response := serveJSON(t, handler, http.MethodPost, "/api/v1/tenants/tenant-a/apps", app)
	if response.Code != http.StatusOK {
		t.Fatalf("POST app status = %d, body = %s", response.Code, response.Body.String())
	}
	var storedApp tenant.AgentApp
	if err := json.NewDecoder(response.Body).Decode(&storedApp); err != nil {
		t.Fatalf("decode app response: %v", err)
	}
	if storedApp.Version != 1 {
		t.Fatalf("app version = %d, want 1", storedApp.Version)
	}

	binding := testBinding()
	response = serveJSON(t, handler, http.MethodPost, "/api/v1/apps/app-a/bindings", binding)
	if response.Code != http.StatusOK {
		t.Fatalf("POST binding status = %d, body = %s", response.Code, response.Body.String())
	}
	if cache.invalidations != 1 {
		t.Fatalf("cache invalidations = %d, want 1", cache.invalidations)
	}

	request := authorizedRequest(t, http.MethodPost, "/api/v1/apps/app-a/versions/1/activate", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("activate app status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestAdminRejectsUnknownJSONField(t *testing.T) {
	handler := newTestHandler(t)
	body := bytes.NewBufferString(`{"tenant_id":"tenant-a","name":"A","unexpected":true}`)
	request := authorizedRequest(t, http.MethodPost, "/api/v1/tenants", body)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	handler, err := NewHandler(newMemoryStore(), nil, "admin", "test-password")
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func serveJSON(t *testing.T, handler http.Handler, method, path string, value any) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	request := authorizedRequest(t, method, path, bytes.NewReader(data))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func authorizedRequest(t *testing.T, method, path string, body io.Reader) *http.Request {
	t.Helper()
	var request *http.Request
	if body == nil {
		request = httptest.NewRequest(method, path, nil)
	} else {
		request = httptest.NewRequest(method, path, body)
	}
	request.SetBasicAuth("admin", "test-password")
	return request
}

type memoryStore struct {
	mu       sync.Mutex
	tenants  map[string]tenant.Tenant
	apps     map[string][]tenant.AgentApp
	bindings map[string]tenant.ChannelBinding
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		tenants:  make(map[string]tenant.Tenant),
		apps:     make(map[string][]tenant.AgentApp),
		bindings: make(map[string]tenant.ChannelBinding),
	}
}

func (m *memoryStore) UpsertTenant(_ context.Context, value tenant.Tenant) error {
	if err := value.Validate(); err != nil {
		return errors.New("validate tenant: " + err.Error())
	}
	m.mu.Lock()
	m.tenants[value.ID] = value
	m.mu.Unlock()
	return nil
}

func (m *memoryStore) GetTenant(_ context.Context, id string) (tenant.Tenant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.tenants[id]
	if !ok {
		return tenant.Tenant{}, tenant.ErrNotFound
	}
	return value, nil
}

func (m *memoryStore) ListTenants(context.Context) ([]tenant.Tenant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]tenant.Tenant, 0, len(m.tenants))
	for _, value := range m.tenants {
		result = append(result, value)
	}
	return result, nil
}

func (m *memoryStore) DeactivateTenant(ctx context.Context, id string) error {
	value, err := m.GetTenant(ctx, id)
	if err != nil {
		return err
	}
	value.IsActive = false
	return m.UpsertTenant(ctx, value)
}

func (m *memoryStore) UpsertApp(_ context.Context, value tenant.AgentApp) (int, error) {
	if err := value.Validate(); err != nil {
		return 0, errors.New("validate app: " + err.Error())
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value.Version = len(m.apps[value.ID]) + 1
	value.IsCurrent = value.Version == 1
	m.apps[value.ID] = append(m.apps[value.ID], value)
	return value.Version, nil
}

func (m *memoryStore) GetCurrentApp(_ context.Context, id string) (tenant.AgentApp, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, value := range m.apps[id] {
		if value.IsCurrent {
			return value, nil
		}
	}
	return tenant.AgentApp{}, tenant.ErrNotFound
}

func (m *memoryStore) ListApps(_ context.Context, tenantID string) ([]tenant.AgentApp, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []tenant.AgentApp
	for _, versions := range m.apps {
		for _, value := range versions {
			if value.TenantID == tenantID && value.IsCurrent {
				result = append(result, value)
			}
		}
	}
	return result, nil
}

func (m *memoryStore) BindAppVersion(_ context.Context, id string, version int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := m.apps[id]
	found := false
	for index := range values {
		values[index].IsCurrent = values[index].Version == version
		found = found || values[index].IsCurrent
	}
	if !found {
		return tenant.ErrNotFound
	}
	m.apps[id] = values
	return nil
}

func (m *memoryStore) UpsertBinding(_ context.Context, value tenant.ChannelBinding) error {
	if err := value.Validate(); err != nil {
		return errors.New("validate binding: " + err.Error())
	}
	m.mu.Lock()
	m.bindings[value.ID] = value
	m.mu.Unlock()
	return nil
}

func (m *memoryStore) GetBindingByRoute(_ context.Context, channel, routeKey string) (tenant.ChannelBinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, value := range m.bindings {
		if value.Channel == channel && value.RouteKey == routeKey {
			return value, nil
		}
	}
	return tenant.ChannelBinding{}, tenant.ErrNotFound
}

func (m *memoryStore) ListBindings(_ context.Context, appID string) ([]tenant.ChannelBinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []tenant.ChannelBinding
	for _, value := range m.bindings {
		if value.AppID == appID {
			result = append(result, value)
		}
	}
	return result, nil
}

type recordingCache struct {
	invalidations int
}

func (c *recordingCache) ResolveBinding(context.Context, string, string) (tenant.Snapshot, error) {
	return tenant.Snapshot{}, tenant.ErrNotFound
}

func (c *recordingCache) Invalidate(string, string) {
	c.invalidations++
}

func testTenant() tenant.Tenant {
	return tenant.Tenant{
		ID:       "tenant-a",
		Name:     "Tenant A",
		IsActive: true,
		Quota: tenant.Quota{
			DailyTokenLimit: 1000,
			RatePerMinute:   30,
		},
		Policy: tenant.Policy{AuditLevel: "full"},
	}
}

func testApp() tenant.AgentApp {
	return tenant.AgentApp{
		ID:       "app-a",
		TenantID: "tenant-a",
		AppName:  "tenant-a-support",
		Model: tenant.ModelConfig{
			Provider:  "openai-compatible",
			Model:     "test-model",
			APIKeyRef: "env:MODEL_KEY_TENANT_A",
		},
		Tools: []string{"search"},
		Backends: tenant.BackendSelection{
			Session: "redis",
			Memory:  "pgvector",
		},
	}
}

func testBinding() tenant.ChannelBinding {
	return tenant.ChannelBinding{
		ID:       "binding-a",
		TenantID: "tenant-a",
		AppID:    "app-a",
		Channel:  "webui",
		RouteKey: "binding-a",
		Config:   map[string]string{"session_key": "env:WEBUI_SESSION_KEY_TENANT_A"},
		IsActive: true,
	}
}
