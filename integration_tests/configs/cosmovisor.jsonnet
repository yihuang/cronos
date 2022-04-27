local config = import '../../scripts/default.jsonnet';

std.manifestYamlDoc(config {
  'dotenv'+: '../../scripts/.env',
  'cronos_777-1'+: {
    'start-flags':: super['start-flags'],
    'app-config'+: {
      'minimum-gas-prices': '5000000000000basetcro',
      'json-rpc'+: {
        api:: super['api'],
        'feehistory-cap': 100,
        'block-range-cap': 10000,
        'logs-cap': 10000,
      },
    },
    genesis+: {
      app_state+: {
        feemarket: {
          params: {
            no_base_fee: true,
          },
        },
      },
    },
  },
}, true, false)
