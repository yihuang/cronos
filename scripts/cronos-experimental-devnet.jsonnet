local config = import 'default.jsonnet';

std.manifestYamlDoc(config {
  'cronos_777-1'+: {
    'start-flags': '--trace --unsafe-experimental',
    'app-config'+: {
      'minimum-gas-prices': '5000000000000basetcro',
      'json-rpc'+: {
        api:: super['api'],
      },
    },
    accounts: [
      {
        name: 'community',
        coins: '10000000000000000000000basetcro',
        mnemonic: 'notable error gospel wave pair ugly measure elite toddler cost various fly make eye ketchup despair slab throw tribe swarm word fruit into inmate',
      },
      {
        name: 'signer1',
        coins: '20000000000000000000000basetcro',
        mnemonic: 'shed crumble dismiss loyal latin million oblige gesture shrug still oxygen custom remove ribbon disorder palace addict again blanket sad flock consider obey popular',
      },
      {
        name: 'signer2',
        coins: '30000000000000000000000basetcro',
        mnemonic: 'night renew tonight dinner shaft scheme domain oppose echo summer broccoli agent face guitar surface belt veteran siren poem alcohol menu custom crunch index',
      },
    ],
    validators: [
      {
        coins: '1000000000000000000stake,10000000000000000000000basetcro',
        staked: '1000000000000000000stake',
        mnemonic: 'visit craft resemble online window solution west chuckle music diesel vital settle comic tribe project blame bulb armed flower region sausage mercy arrive release',
      },
      {
        coins: '1000000000000000000stake,10000000000000000000000basetcro',
        staked: '1000000000000000000stake',
        mnemonic: 'direct travel shrug hand twice agent sail sell jump phone velvet pilot mango charge usual multiply orient garment bleak virtual action mention panda vast',
      },
    ],
    genesis+: {
      app_state+: {
        cronos: {
          params: {
            cronos_admin: 'crc12luku6uxehhak02py4rcz65zu0swh7wjsrw0pp',
            enable_auto_deployment: true,
            ibc_cro_denom: 'ibc/6411AE2ADA1E73DB59DB151A8988F9B7D5E7E233D8414DB6817F8F1A01611F86',
          },
        },
      },
    },
  },
}, true, false)
