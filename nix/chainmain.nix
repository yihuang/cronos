{ system }:
(import
  (builtins.fetchTarball {
    url = "https://github.com/crypto-org-chain/chain-main/archive/v2.1.2.tar.gz";
    sha256 = sha256:0nyxzzhyn3aqg1hy1il96gsbf4wcjdb9lrivfh5j3hij2xqs7h1k;
  })
  { inherit system; }).chain-maind
