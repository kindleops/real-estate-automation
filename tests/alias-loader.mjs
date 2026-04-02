import path from "node:path";
import { pathToFileURL } from "node:url";

const projectRoot = process.cwd();

export async function resolve(specifier, context, defaultResolve) {
  if (specifier.startsWith("@/")) {
    const resolvedPath = path.join(projectRoot, "src", specifier.slice(2));
    return {
      url: pathToFileURL(resolvedPath).href,
      shortCircuit: true,
    };
  }

  return defaultResolve(specifier, context, defaultResolve);
}
