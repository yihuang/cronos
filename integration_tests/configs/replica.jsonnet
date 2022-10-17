local config = import 'primary.jsonnet';

config {
  'cronos_777-1'+: {
    'app-config'+: {
      'json-rpc': {
        enable: false,
      },
      'grpc-only': true,
    },
  },
}
