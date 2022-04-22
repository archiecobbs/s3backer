project('s3backer', ['c'], version: '1.6.2',
        meson_version: '>= 0.47',
        default_options: [
            'buildtype=debugoptimized',
            'warning_level=2',
        ])


# TODO: Detect if there is an existing config.h (created by ./configure) in
# the source directory and, if so, either remove it or abort.

cc = meson.get_compiler('c')

#
# Feature detection
#
cfg = configuration_data()
cfg.set_quoted('PACKAGE', meson.project_name())
cfg.set_quoted('PACKAGE_VERSION', meson.project_version())
cfg.set_quoted('VERSION', meson.project_version())
cfg.set('FUSE_USE_VERSION', 26)
cfg.set('_GNU_SOURCE', 1)

git_desc = run_command('git', 'describe', check: true)
cfg.set_quoted('GIT_REV', git_desc.stdout().strip())

configure_file(output: 'config.h',
               configuration : cfg)

#
# Compiler configuration
#
add_project_arguments('-DHAVE_CONFIG_H', 
                      '-Wno-sign-compare', '-Wno-unused-parameter',
                      '-Wstrict-prototypes', '-Wmissing-declarations', '-Wwrite-strings',
                      '-fno-strict-aliasing', language: 'c')


# Some (stupid) GCC versions warn about unused return values even when they are
# casted to void. This makes -Wunused-result pretty useless, since there is no
# way to suppress the warning when we really *want* to ignore the value.
code = '''
__attribute__((warn_unused_result)) int get_4() {
    return 4;
}
int main(void) {
    (void) get_4();
    return 0;
}'''
if not cc.compiles(code, args: [ '-O0', '-Werror=unused-result' ])
     message('Compiler warns about unused result even when casting to void')
     add_project_arguments('-Wno-unused-result', language: 'c')
endif


#
# Dependencies
#
thread_dep = dependency('threads') 
expat_dep = dependency('expat') 
libcurl_dep = dependency('libcurl') 
zlib_dep = dependency('zlib') 
ssl_dep = dependency('libcrypto') 
nbdkit_dep = dependency('nbdkit') 
fuse_dep = dependency('fuse') 

#
# Targets
#

common_sources = [ 'block_cache.c', 'block_part.c', 'dcache.c',
                   'ec_protect.c', 'zero_cache.c', 'erase.c', 'fuse_ops.c',
                   'hash.c', 'util.c', 'http_io.c', 'reset.c', 's3b_config.c',
                   'test_io.c', 'sslcompat.c', 'gitrev2.c' ] 

s3backer = executable('s3backer', 
    [ 'main.c' ] + common_sources,
    include_directories: '.',
    dependencies: [thread_dep, expat_dep, libcurl_dep, zlib_dep, ssl_dep, fuse_dep], 
   install: true,
)

tester = executable('tester', 
    [ 'tester.c' ] + common_sources,
    include_directories: '.',
    dependencies: [thread_dep, expat_dep, libcurl_dep, zlib_dep, ssl_dep, fuse_dep], 
   install: true,
)

nbdkit_s3backer_plugin = shared_library('nbdkit-s3backer-plugin', 
   [ 'nbdkit.c' ] + common_sources, 
   dependencies: [thread_dep, expat_dep, libcurl_dep, zlib_dep, ssl_dep, nbdkit_dep], 
   override_options: [ 'b_lundef=false' ],
   install: true,
   install_dir: run_command(
      'pkg-config', 'nbdkit', '--variable=plugindir', check: true).stdout().strip(),
   name_prefix: '',
)


