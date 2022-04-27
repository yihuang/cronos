local config = import '../../scripts/default.jsonnet';

std.manifestYamlDoc(config {
  'dotenv'+: '../../scripts/.env',
  'cronos_777-1'+: {
    'app-config'+: {
      evm+: {
        'max-tx-gas-wanted': 0,
      },
    },
    genesis+: {
      consensus_params+: {
        block+: {
          max_gas: '815000',
        },
      },
    },
  },
}, true, false)
