FROM swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:dep-runbox7-ca6df8e5-ae4449b8e848

WORKDIR /app/src

# Match the Angular CLI compatibility-compiler invocation observed during the
# isolated Harbor check. This writes the Ivy entry points into the immutable
# dependency base so the 2 GiB task container does not fan out ngcc workers on
# its first test invocation.
RUN ./node_modules/.bin/ngcc \
    --source /app/src/node_modules \
    --properties browser module main \
    --first-only \
    --create-ivy-entry-points \
    --async \
    --tsconfig /app/src/src/tsconfig.spec.json \
    --loglevel info \
    && test ! -e /app/src/node_modules/@angular/compiler-cli/ngcc/__ngcc_lock_file__
