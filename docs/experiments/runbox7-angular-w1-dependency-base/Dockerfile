FROM swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:dep-runbox7-ca6df8e5-ae4449b8e848-ngcc

WORKDIR /app/src

# Angular CLI 9 reads the host CPU count instead of the container's one-CPU
# quota and otherwise launches seven build workers. Keep the task's 2 GiB
# memory contract and align Angular's internal worker count with its CPU quota.
RUN workers_js=/app/src/node_modules/@angular-devkit/build-angular/src/utils/workers.js \
    && grep -Fq 'exports.maxWorkers = Math.max(Math.min(os_1.cpus().length, 8) - 1, 1);' "$workers_js" \
    && sed -i \
        's/exports.maxWorkers = Math.max(Math.min(os_1.cpus().length, 8) - 1, 1);/exports.maxWorkers = 1;/' \
        "$workers_js" \
    && grep -Fq 'exports.maxWorkers = 1;' "$workers_js"
