# build a binary package that can be distributed to non-nix environment
{ system ? builtins.currentSystem, pkgs ? import ./nix { inherit system; } }:
with pkgs;
let cronosd = import ./. { inherit system pkgs; };
in
stdenv.mkDerivation {
  name = cronosd.name + "-dist-" + system;
  unpackPhase = "true";
  buildPhase = "true";
  installPhase = ''
    mkdir -p $out/{bin,lib}
    cp ${cronosd}/bin/cronosd $out/bin/
    chmod +w $out/bin/cronosd
  '' + lib.optionalString stdenv.isDarwin ''
    # copy dynamic libraries
    cp "${libcxx}/lib/libc++.1.0.dylib" $out/lib/libc++.dylib
    # patch binary
    install_name_tool -change "${libcxx}/lib/libc++.1.0.dylib" "libc++.dylib" $out/bin/cronosd
    install_name_tool -add_rpath "@executable_path/../lib" $out/bin/cronosd
  '' + lib.optionalString stdenv.isLinux ''
    # copy dynamic libraries
    libc=$(patchelf --print-rpath $out/bin/cronosd | cut -d ':' -f 1)
    libgcc=$(patchelf --print-rpath $out/bin/cronosd | cut -d ':' -f 2)
    echo "copy libstdc++.so from ''${libgcc}"
    cp "''${libgcc}/libstdc++.so.6" $out/lib/
    echo "copy c libraries from ''${libc}"
    cp ''${libc}/{libm.so.6,libdl.so.2,libpthread.so.0,libgcc_s.so.1,libc.so.6} $out/lib/
    # patch binary
    patchelf --set-interpreter '/lib64/ld-linux-x86-64.so.2' $out/bin/cronosd
    patchelf --set-rpath '$ORIGIN/../lib' $out/bin/cronosd
  '';
  fixupPhase = "true";
}
