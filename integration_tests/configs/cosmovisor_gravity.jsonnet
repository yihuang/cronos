local config = import 'default.jsonnet';

config {
  'cronos_777-1'+: {
    'app-config'+: {
      'minimum-gas-prices': '100000000000basetcro',
      'iavl-lazy-loading':: super['iavl-lazy-loading'],
      store:: super.store,
      streamers:: super.streamers,
    },
    genesis+: {
      app_state+: {
        feemarket+: {
          params+: {
            no_base_fee: true,
          },
        },
      },
    },
  },
}
