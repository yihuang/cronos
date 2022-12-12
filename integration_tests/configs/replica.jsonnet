local config = import 'primary.jsonnet';

config {
  'cronos_777-1'+: {
    'app-config'+: {
      'json-rpc': {
        enable: false,
      },
      'is-local': false,
      'remote-url': 'http://0.0.0.0:8080',
      'remote-grpc-url': 'http://0.0.0.0:26754',
      'remote-ws-url': 'ws://0.0.0.0:26767/websocket',
      'grpc-only': true,
      'concurrency': 6,
    },
  },
}
