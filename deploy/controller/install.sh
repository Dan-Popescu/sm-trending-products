#!/usr/bin/env bash
set -euo pipefail

if [[ $(id -u) != 0 ]]; then
  echo 'Run this installer in the Hostinger root console.' >&2
  exit 1
fi

controller_python=/opt/sm-trends/controller/bin/python
controller_config=/etc/sm-trends/release-controller.json
template_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

test -x "$controller_python"
test -f "$controller_config"
install -d -m 0700 /var/lib/sm-trends/release
install -m 0644 "$template_directory/sm-trends-release.service" /etc/systemd/system/
install -m 0644 "$template_directory/sm-trends-release.timer" /etc/systemd/system/
systemctl daemon-reload
# Refuse to arm polling until the initial signed, running slot was adopted.
"$controller_python" -m sm_trending_products.release_cli status --config "$controller_config"
systemctl enable --now sm-trends-release.timer
echo 'Signed deployment polling enabled; inspect journalctl -u sm-trends-release.service.'
