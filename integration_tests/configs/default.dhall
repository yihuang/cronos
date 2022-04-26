let env = ./env.dhall
in { cronos_777-1 =
  { cmd = "cronosd"
  , start-flags = "--trace"
  , app-config =
    { minimum-gas-prices = "0basetcro"
    , json-rpc =
      { address = "0.0.0.0={EVMRPC_PORT}"
      , ws-address = "0.0.0.0={EVMRPC_PORT_WS}"
      , api = "eth,net,web3,debug"
      }
    }
  , validators =
    [ { coins = "1000000000000000000stake,10000000000000000000000basetcro"
      , staked = "1000000000000000000stake"
      , mnemonic = env.VALIDATOR1_MNEMONIC
      }
    , { coins = "1000000000000000000stake,10000000000000000000000basetcro"
      , staked = "1000000000000000000stake"
      , mnemonic = env.VALIDATOR2_MNEMONIC
      }
    ]
  , accounts =
    [ { name = "community"
      , coins = "10000000000000000000000basetcro"
      , mnemonic = env.COMMUNITY_MNEMONIC
      }
    , { name = "signer1"
      , coins = "20000000000000000000000basetcro"
      , mnemonic = env.SIGNER1_MNEMONIC
      }
    , { name = "signer2"
      , coins = "30000000000000000000000basetcro"
      , mnemonic = env.SIGNER2_MNEMONIC
      }
    ]
  , genesis = {=},
  }
}
