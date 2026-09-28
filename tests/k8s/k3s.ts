// A throwaway Kubernetes for tests: k3s in one Docker container. Nothing on
// the host is configured (no kind, no kubeconfig): kubectl runs inside the
// container. The cluster is kept between runs (about 13 s to create, images
// are imported only when they changed); K3S_RESET=1 recreates it,
// `docker rm -f pgtxn-k3s` removes it.
import { execFileSync } from "node:child_process";

export const K3S = "pgtxn-k3s";
const IMAGE = "rancher/k3s:v1.33.4-k3s1";
// NodePorts published on the host
export const PORTS = { postgres: [55450, 30432], receiver: [55451, 30808] } as const;

export function docker(args: string[], input?: string | Buffer): string {
  return execFileSync("docker", args, { encoding: "utf8", input, stdio: [input ? "pipe" : "ignore", "pipe", "pipe"], maxBuffer: 1 << 26 });
}

export function kubectl(args: string[], input?: string): string {
  return docker(["exec", ...(input ? ["-i"] : []), K3S, "kubectl", ...args], input);
}

function running(): boolean {
  try {
    return docker(["inspect", "-f", "{{.State.Running}}", K3S]).trim() === "true";
  } catch {
    return false;
  }
}

function publishes(): boolean {
  try {
    return Object.values(PORTS).every(([host, node]) => docker(["port", K3S, `${node}/tcp`]).includes(`:${host}`));
  } catch {
    return false;
  }
}

function ready(): boolean {
  try {
    kubectl(["get", "--raw=/readyz"]);
    kubectl(["get", "serviceaccount", "default"]);
    return kubectl(["get", "nodes", "--no-headers"]).includes(" Ready ");
  } catch {
    return false;
  }
}

export async function ensureCluster(): Promise<void> {
  if (process.env.K3S_RESET) docker(["rm", "-f", K3S]);
  if (running() && !publishes()) docker(["rm", "-f", K3S]);
  if (!running()) {
    try {
      docker(["rm", "-f", K3S]);
    } catch {
      // not there
    }
    docker(["run", "-d", "--name", K3S, "--privileged", "--tmpfs", "/run", "--tmpfs", "/var/run",
      ...Object.values(PORTS).flatMap(([host, node]) => ["-p", `${host}:${node}`]),
      IMAGE, "server", "--disable=traefik,metrics-server,local-storage", "--disable-helm-controller"]);
  }
  const deadline = Date.now() + 120_000;
  while (!ready()) {
    if (Date.now() > deadline) throw new Error("k3s did not become ready");
    await new Promise((r) => setTimeout(r, 500));
  }
}

/** Makes a local Docker image available to the cluster (imagePullPolicy: Never). */
export function loadImage(image: string): void {
  const id = docker(["image", "inspect", "-f", "{{.Id}}", image]).trim();
  const known = kubectl(["get", "nodes", "-o", "jsonpath={.items[0].status.images[*].names}"]);
  const have = docker(["exec", K3S, "crictl", "images", "-q", "--no-trunc"]);
  if (have.includes(id) || known.includes(id)) return;
  execFileSync("bash", ["-c", `docker save ${image} | docker exec -i ${K3S} ctr -n k8s.io images import - >/dev/null`]);
}
