// Fails fast, with a clear message, when the consumer runs on the wrong Node version.
//
// The Kafka client loads a prebuilt native binary that only works on the Node ABI it was
// built for (Node 24 here; see .node-version and the README). On another version it fails
// with an opaque "NODE_MODULE_VERSION" error.
//
// This must be the consumer's first import. ES module imports are evaluated in the order
// they are declared, before the importing file's own code, so a check written inside
// index.ts would run after the Kafka client had already tried to load.
const REQUIRED_MAJOR = 24;

const major = Number(process.versions.node.split('.')[0]);
if (major !== REQUIRED_MAJOR) {
  console.error(
    `consumer: needs Node ${REQUIRED_MAJOR}, found ${process.version}. The Kafka client's native ` +
      `binary is built for Node ${REQUIRED_MAJOR}. Switch with your version manager ` +
      `(the repo's .node-version says ${REQUIRED_MAJOR}), e.g. "fnm use".`,
  );
  process.exit(1);
}
