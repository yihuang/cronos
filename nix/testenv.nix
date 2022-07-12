{ poetry2nix, python39 }:
poetry2nix.mkPoetryEnv {
  projectDir = ../integration_tests;
  python = python39;
  overrides = poetry2nix.overrides.withDefaults (self: super: {
    eth-bloom = super.eth-bloom.overridePythonAttrs {
      preConfigure = ''
        substituteInPlace setup.py --replace \'setuptools-markdown\' ""
      '';
    };

    pystarport = super.pystarport.overridePythonAttrs (
      old: {
        nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.poetry ];
      }
    );
  });
}
