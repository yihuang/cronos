local config = import 'primary.jsonnet';

config {
  'cronos_777-1'+: {
    'app-config'+: {
      'json-rpc': {
        enable: false,
      },
      'is-local': false,
      'remote-url': 'http://localhost:8080',
      'grpc-only': true,
      'concurrency': 1,
    },
  },
}
