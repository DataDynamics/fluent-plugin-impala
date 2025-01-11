lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name = "fluent-plugin-impala"
  spec.version = "0.1.0"
  spec.authors = ["김병곤"]
  spec.email = ["fharenheit@gmail.com"]

  spec.summary = %q{Fluentd Plugin for parsing Impala and Kudu logs}
  spec.description = %q{Fluentd Plugin for parsing Impala and Kudu logs}
  spec.homepage = "http://www.opencloudengine.org"
  spec.license = "Apache-2.0"

  spec.files = Dir['lib/**/*', 'spec/**/*', 'vendor/**/*', '*.gemspec', '*.md', 'CONTRIBUTORS', 'Gemfile', 'LICENSE', 'NOTICE.TXT']
  spec.executables = files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files = spec.test_files = s.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 2.6.2"
  spec.add_development_dependency "rake", "~> 13.0.6"
  spec.add_development_dependency "test-unit", "~> 3.5.7"
  spec.add_runtime_dependency "fluentd", [">= 0.14.10", "< 2"]
end
