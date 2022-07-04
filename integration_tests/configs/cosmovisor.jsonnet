local config = import 'default.jsonnet';

config {
  'cronos_777-1'+: {
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
}
