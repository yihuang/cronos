{ writeShellScriptBin
, symlinkJoin
, pystarport
, chainmain
, go-ethereum
, hermes
, config
}: rec {
  start-chainmain = writeShellScriptBin "start-chainmain" ''
    export PATH=${pystarport}/bin:${chainmain}/bin:$PATH
    ${../scripts/start-chainmain} ${config.chainmain-config} ${config.dotenv} $@
  '';
  start-cronos = writeShellScriptBin "start-cronos" ''
    # rely on environment to provide cronosd
    export PATH=${pystarport}/bin:$PATH
    ${../scripts/start-cronos} ${config.cronos-config} ${config.dotenv} $@
  '';
  start-geth = writeShellScriptBin "start-geth" ''
    export PATH=${go-ethereum}/bin:$PATH
    source ${config.dotenv}
    ${../scripts/start-geth} ${config.geth-genesis} $@
  '';
  start-hermes = writeShellScriptBin "start-hermes" ''
    export PATH=${hermes}/bin:$PATH
    source ${config.dotenv}
    ${../scripts/start-hermes} ${config.hermes-config} $@
  '';
  start-scripts = symlinkJoin {
    name = "start-scripts";
    paths = [ start-cronos start-geth start-chainmain start-hermes ];
  };
}
