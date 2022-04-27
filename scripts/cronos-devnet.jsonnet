local config = import 'default.jsonnet';

std.manifestYamlDoc(config {
  'dotenv'+: '.env',
}, true, false)
