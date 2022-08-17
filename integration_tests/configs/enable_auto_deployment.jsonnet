local disable_auto_deployment = import 'disable_auto_deployment.jsonnet';

disable_auto_deployment {
  'cronos_777-1'+: {
    genesis+: {
      app_state+: {
        cronos+: {
          params+: {
            enable_auto_deployment: true,
          },
        },
      },
    },
  },
}
