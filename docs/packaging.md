# Git-backed distribution

Independent Windows consumer failed with ERR_UNSUPPORTED_NODE_MODULES_TYPE_STRIPPING on Node 26.13.0. Public root now points to compiled ESM and declarations. Maintainer runs corepack pnpm build and commits dist/; CI rebuilds and verifies tracked output. No dependency install lifecycle script or consumer transpilation required. Until the first generated dist commit is pushed, this candidate is not installable. AI has not executed the build.
