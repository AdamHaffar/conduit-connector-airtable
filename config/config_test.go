package config

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	testApiKey    = "key01234567890123"
	testBaseID    = "app01234567890123"
	testTableID   = "tbl01234567890123"
	testEnableCDC = "f"
)

func TestParseConfig(t *testing.T) {

	for _, tt := range []struct {
		name  string
		error string
		cfg   map[string]string
	}{
		{
			name:  "cfg map is empty",
			error: fmt.Sprintf("map must not be empty"),
			cfg:   map[string]string{},
		},
		{
			name:  "API key is empty",
			error: fmt.Sprintf("%q config value must be set", APIKey),
			cfg: map[string]string{
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Base ID is empty",
			error: fmt.Sprintf("%q config value must be set", BaseID),
			cfg: map[string]string{
				APIKey:           testApiKey,
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Table ID is empty",
			error: fmt.Sprintf("%q config value must be set", TableID),
			cfg: map[string]string{
				APIKey:           testApiKey,
				BaseID:           testBaseID,
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Enable CDC is empty",
			error: fmt.Sprintf("%q config value must be set", EnableCDC),
			cfg: map[string]string{
				APIKey:           testApiKey,
				BaseID:           testBaseID,
				TableID:          testTableID,
				"nonExistentKey": "value",
			},
		},
		{
			name:  "API key has an invalid prefix",
			error: fmt.Sprintf("id must start with key"),
			cfg: map[string]string{
				APIKey:           "foo01234567890123",
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Base ID has an invalid prefix",
			error: fmt.Sprintf("id must start with app"),
			cfg: map[string]string{
				APIKey:           testApiKey,
				BaseID:           "foo01234567890123",
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Table ID has an invalid prefix",
			error: fmt.Sprintf("id must start with tbl"),
			cfg: map[string]string{
				APIKey:           testApiKey,
				BaseID:           testBaseID,
				TableID:          "foo01234567890123",
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Enable CDC is invalid",
			error: fmt.Sprintf("strconv.ParseBool: parsing \"yes\": invalid syntax"),
			cfg: map[string]string{
				APIKey:    testApiKey,
				BaseID:    testBaseID,
				TableID:   testTableID,
				EnableCDC: "yes",
			},
		},

		{
			name:  "API key has an invalid length",
			error: fmt.Sprintf("id must be 17 characters long"),
			cfg: map[string]string{
				APIKey:           "key01",
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Base ID has an invalid length",
			error: fmt.Sprintf("id must be 17 characters long"),
			cfg: map[string]string{
				APIKey:           testApiKey,
				BaseID:           "app01",
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Table ID has an invalid length",
			error: fmt.Sprintf("id must be 17 characters long"),
			cfg: map[string]string{
				APIKey:           testApiKey,
				BaseID:           testBaseID,
				TableID:          "tbl01",
				"nonExistentKey": "value",
			},
		},
	} {
		t.Run(fmt.Sprintf("Fails when: %s", tt.name), func(t *testing.T) {
			_, err := ParseBaseConfig(tt.cfg)

			require.EqualError(t, err, tt.error)
		})
	}

	t.Run("Returns config when all required config values were provided", func(t *testing.T) {
		cfgRaw := map[string]string{
			APIKey:    testApiKey,
			BaseID:    testBaseID,
			TableID:   testTableID,
			EnableCDC: testEnableCDC,
		}
		config, err := ParseBaseConfig(cfgRaw)

		require.NoError(t, err)
		require.Equal(t, cfgRaw[APIKey], config.APIKey)
		require.Equal(t, cfgRaw[BaseID], config.BaseID)
		require.Equal(t, cfgRaw[TableID], config.TableID)
		require.Equal(t, false, config.EnableCDC)
	})
}
