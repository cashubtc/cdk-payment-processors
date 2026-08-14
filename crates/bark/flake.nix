{
  description = "CDK Payment Processor for Bark Flake";

  nixConfig = {
    extra-substituters = [
      "https://cache.cashudevkit.org"
      "https://cashudevkit.cachix.org"
      "https://bark.cachix.org"
    ];
    extra-trusted-public-keys = [
      "cashudevkit:Ukc9ltM4674fDHWWay+q4vdHDYKF48QIm6A+0z5/FqQ="
      "cashudevkit.cachix.org-1:zFKdvMiTllKWxIFNTjXgisZsOFufmaZXjWJNcmc8r+4="
      "bark.cachix.org-1:Iaihe4ABbOQz1CHBoYUZS/sHVAcISasJZ+lL3I4gRB0="
    ];
  };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };

    flake-utils.url = "github:numtide/flake-utils";

    pre-commit-hooks.url = "github:cachix/pre-commit-hooks.nix";

    # Keep the daemon binaries and test harness on the exact Bark revision
    # used by the optional `ark-testing` Cargo dependency.
    bark-upstream.url = "git+https://gitlab.com/ark-bitcoin/bark.git?ref=refs/tags/bark-0.6.1";
  };

  outputs =
    { self
    , nixpkgs
    , rust-overlay
    , flake-utils
    , pre-commit-hooks
    , ...
    }@inputs:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        lib = pkgs.lib;
        stdenv = pkgs.stdenv;
        isDarwin = stdenv.isDarwin;
        libsDarwin = lib.optionals isDarwin [
          # Additional drwin specific inputs can be set here
          # Note: Security and SystemConfiguration frameworks are provided by the default SDK
        ];

        # Dependencies
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        barkTestShell = inputs.bark-upstream.devShells.${system}.default;
        barkTestBitcoinBin = builtins.dirOf barkTestShell.BITCOIND_EXEC;
        barkTestEnv = {
          inherit (barkTestShell)
            POSTGRES_BINS
            BITCOIND_EXEC
            ESPLORA_ELECTRS_EXEC
            LD_LIBRARY_PATH
            LIGHTNINGD_EXEC
            LIGHTNINGD_DOCKER_IMAGE
            LIGHTNINGD_PLUGIN_DIR
            ;
        };

        # Toolchains
        # latest stable
        stable_toolchain = pkgs.rust-bin.stable."1.95.0".default.override {
          targets = [ "wasm32-unknown-unknown" ]; # wasm
          extensions = [
            "rustfmt"
            "clippy"
            "rust-analyzer"
          ];
        };

        # MSRV stable
        msrv_toolchain = pkgs.rust-bin.stable."1.85.0".default.override {
          targets = [ "wasm32-unknown-unknown" ]; # wasm
          extensions = [
            "rustfmt"
            "clippy"
            "rust-analyzer"
          ];
        };

        # Nightly used for formatting
        nightly_toolchain = pkgs.rust-bin.selectLatestNightlyWith (
          toolchain:
          toolchain.default.override {
            extensions = [
              "rustfmt"
              "clippy"
              "rust-analyzer"
              "rust-src"
            ];
            targets = [ "wasm32-unknown-unknown" ]; # wasm
          }
        );

        # Common inputs
        envVars = {
          # rust analyzer needs  NIX_PATH for some reason.
          NIX_PATH = "nixpkgs=${inputs.nixpkgs}";
        };
        buildInputs =
          with pkgs;
          [
            # Add additional build inputs here
            git
            pkg-config
            curl
            just
            protobuf
            nixpkgs-fmt
            typos
            lnd
            clightning
            bitcoind
            sqlx-cli
            cargo-outdated
            mprocs
            sqlite

            # Needed for github ci
            libz
          ]
          ++ libsDarwin;

        # Common arguments can be set here to avoid repeating them later
        nativeBuildInputs = [
          #Add additional build inputs here
        ]
        ++ lib.optionals isDarwin [
          # Additional darwin specific native inputs can be set here
        ];
      in
      {
        checks = {
          # Pre-commit checks
          pre-commit-check =
            let
              # this is a hack based on https://github.com/cachix/pre-commit-hooks.nix/issues/126
              # we want to use our own rust stuff from oxalica's overlay
              _rust = pkgs.rust-bin.stable.latest.default;
              rust = pkgs.buildEnv {
                name = _rust.name;
                inherit (_rust) meta;
                buildInputs = [ pkgs.makeWrapper ];
                paths = [ _rust ];
                pathsToLink = [
                  "/"
                  "/bin"
                ];
                postBuild = ''
                  for i in $out/bin/*; do
                    wrapProgram "$i" --prefix PATH : "$out/bin"
                  done
                '';
              };
            in
            pre-commit-hooks.lib.${system}.run {
              src = ./.;
              hooks = {
                rustfmt = {
                  enable = true;
                  entry = lib.mkForce "${rust}/bin/cargo-fmt fmt --all -- --config format_code_in_doc_comments=true --check --color always";
                };
                nixpkgs-fmt.enable = true;
                typos.enable = true;
                commitizen.enable = true; # conventional commits
              };
            };
        };

        devShells =
          let
            # pre-commit-checks
            _shellHook = (self.checks.${system}.pre-commit-check.shellHook or "");

            # devShells
            msrv = pkgs.mkShell (
              {
                shellHook = "
              ${_shellHook}
              ";
                buildInputs = buildInputs ++ [ msrv_toolchain ];
                inherit nativeBuildInputs;
              }
              // envVars
            );

            stable = pkgs.mkShell (
              {
                shellHook = ''${_shellHook}'';
                buildInputs = buildInputs ++ [ stable_toolchain ];
                inherit nativeBuildInputs;

              }
              // envVars
            );

            nightly = pkgs.mkShell (
              {
                shellHook = ''
                  ${_shellHook}
                  # Needed for github ci
                  export LD_LIBRARY_PATH=${
                    pkgs.lib.makeLibraryPath [
                      pkgs.zlib
                    ]
                  }:$LD_LIBRARY_PATH
                '';
                buildInputs = buildInputs ++ [ nightly_toolchain ];
                inherit nativeBuildInputs;
              }
              // envVars
            );

            # Shell with the exact Bitcoin Core, Esplora, PostgreSQL, and CLN
            # binaries expected by Bark's pinned test harness. Cargo and rustc
            # deliberately come from the caller's PATH; CI installs Rust
            # before entering this runtime-only shell.
            integration = pkgs.mkShell (
              {
                shellHook = ''
                  export CHAIN_SOURCE=esplora
                  export PATH="${barkTestBitcoinBin}:$PATH"
                  if [ -n "''${LIGHTNINGD_DOCKER_IMAGE:-}" ]; then
                    if ! docker info >/dev/null 2>&1; then
                      echo "The Bark Regtest shell needs a running Docker daemon for Core Lightning on this platform."
                      exit 1
                    fi
                  fi
                  echo "Bark Regtest services are available; run: just test-regtest"
                '';
                # The service paths below come from Bark's pinned shell. Do
                # not inherit that complete developer shell: it also contains
                # browsers, Java, LND, editor tooling, and unrelated daemons.
                buildInputs =
                  with pkgs;
                  [
                    just
                    pkg-config
                    protobuf
                    libz
                  ];
                inherit nativeBuildInputs;
              }
              // envVars
              // barkTestEnv
            );

          in
          {
            inherit
              msrv
              stable
              nightly
              integration
              ;
            default = stable;
          };
      }
    );
}
