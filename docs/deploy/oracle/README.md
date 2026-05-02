# Pivotable on Oracle Cloud Always-Free

Oracle's free tier gives every account **4 ARM Ampere A1-Flex vCPUs + 24 GB RAM, forever**, across any number of VMs. That's more than enough to run Pivotable with room to spare, permanently, for $0.

Trade-off vs. Render: ~1 h of one-time setup, then zero ongoing cost and no cold starts. You're responsible for OS patching and TLS renewal (the walkthrough below wires in Let's Encrypt auto-renewal).

## 1. Create the VM (~15 min)

1. Sign up at [cloud.oracle.com](https://cloud.oracle.com). Always-Free is the default for new accounts.
2. Console → **Compute** → **Instances** → **Create instance**.
3. Shape: **Ampere → VM.Standard.A1.Flex**, 2 OCPUs / 12 GB RAM (well within the free envelope, leaves headroom for a second VM later).
4. Image: **Canonical Ubuntu 24.04**. Save the generated SSH key.
5. Networking → assign a public IPv4. Note it down.
6. Create. The VM boots in ~30 s.

## 2. Open the HTTP port (~2 min)

Oracle's default VCN security list blocks everything. Console → **Networking** → **VCN** → your subnet → **Security List** → **Add ingress rules**:

| Source CIDR | IP Protocol | Destination port |
|-------------|-------------|------------------|
| 0.0.0.0/0   | TCP         | 80               |
| 0.0.0.0/0   | TCP         | 443              |

*Don't* open 8080 — the app binds to localhost only, nginx front-ends it (see step 5).

Also on the VM itself:

```
sudo ufw allow 80/tcp && sudo ufw allow 443/tcp && sudo ufw allow OpenSSH
sudo ufw enable
```

## 3. Install Java 25 (~3 min)

```
sudo apt update
sudo apt install -y wget ca-certificates

# Temurin 25 via Adoptium's APT repo (ARM64 build included).
wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public | \
	sudo tee /etc/apt/trusted.gpg.d/adoptium.asc
echo "deb https://packages.adoptium.net/artifactory/deb $(lsb_release -cs) main" | \
	sudo tee /etc/apt/sources.list.d/adoptium.list
sudo apt update
sudo apt install -y temurin-25-jre
java --version   # sanity-check
```

## 4. Build + upload the fat-jar (~5 min)

From your development machine:

```
mvn -pl :pivotable-server-webflux -am package \
	-DskipTests -Dspotless.skip=true -Denforcer.skip=true -Dcyclonedx.skip=true \
	-Dcheckstyle.skip=true -Dpmd.skip=true -Dspotbugs.skip=true

scp -i ~/.ssh/oracle.key \
	pivotable/server-webflux/target/pivotable-server-webflux-*-exec.jar \
	ubuntu@<PUBLIC_IP>:/tmp/app.jar
```

On the VM:

```
sudo useradd --system --shell /usr/sbin/nologin --home /opt/pivotable pivotable
sudo mkdir -p /opt/pivotable
sudo mv /tmp/app.jar /opt/pivotable/app.jar
sudo chown -R pivotable:pivotable /opt/pivotable
```

## 5. Wire the systemd unit (~2 min)

The unit file lives next to this README. Copy it onto the VM (via `scp` or straight from the repo) and enable:

```
sudo cp pivotable.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pivotable
sudo systemctl status pivotable
journalctl -u pivotable -f         # follow startup logs
```

The service binds to `127.0.0.1:8080` only — nothing on the public internet reaches it directly yet.

## 6. nginx + Let's Encrypt (~10 min)

```
sudo apt install -y nginx certbot python3-certbot-nginx
```

Create `/etc/nginx/sites-available/pivotable`:

```
server {
	listen 80;
	server_name pivotable.example.com;          # your DNS

	location / {
		proxy_pass http://127.0.0.1:8080;
		proxy_http_version 1.1;
		proxy_set_header Host $host;
		proxy_set_header X-Real-IP $remote_addr;
		proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
		proxy_set_header X-Forwarded-Proto $scheme;
	}
}
```

Activate + enable HTTPS:

```
sudo ln -s /etc/nginx/sites-available/pivotable /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
sudo certbot --nginx -d pivotable.example.com   # prompts once; auto-renews via systemd timer
```

## 7. Redeploys (~30 s)

```
scp -i ~/.ssh/oracle.key \
	pivotable/server-webflux/target/pivotable-server-webflux-*-exec.jar \
	ubuntu@<PUBLIC_IP>:/tmp/app.jar
ssh -i ~/.ssh/oracle.key ubuntu@<PUBLIC_IP> '
	sudo mv /tmp/app.jar /opt/pivotable/app.jar &&
	sudo chown pivotable:pivotable /opt/pivotable/app.jar &&
	sudo systemctl restart pivotable
'
```

Script this into a `deploy.sh` in your shell-history folder once you've done it twice.

## Rollback

systemd keeps the old jar in `.jar~` only if you copied with `cp -b`; easier is to timestamp jars (`app-YYYYMMDD.jar`) and flip the `ExecStart` symlink. For a throwaway demo, just re-upload the previous build from your Maven local repo (`~/.m2/repository/eu/solven/adhoc/pivotable-server-webflux/`).

## Cost watch

Always-Free is generous but not infinite — if you provision **more** A1-Flex capacity than the free envelope (4 OCPU / 24 GB), you start paying. The VM sizing in step 1 (2 OCPU / 12 GB) leaves you a second identical VM in reserve at no cost.
