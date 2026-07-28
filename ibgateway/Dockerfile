FROM ghcr.io/gnzsnz/ib-gateway:stable

# IB Gateway listens on 4002 (paper) or 4001 (live)
# Controlled via IB_TRADING_MODE env var set in Railway
EXPOSE 4002
EXPOSE 4001

# Healthcheck — confirms Gateway process is alive
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD curl -f http://localhost:5000/healthz || exit 1
