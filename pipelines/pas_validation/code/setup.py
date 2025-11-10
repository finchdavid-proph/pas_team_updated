from setuptools import setup, find_packages
setup(
    name = 'pas_validation',
    version = '1.0',
    packages = find_packages(include = ('pas_validation*', )) + ['prophecy_config_instances.pas_validation'],
    package_dir = {'prophecy_config_instances.pas_validation' : 'configs/resources/pas_validation'},
    package_data = {'prophecy_config_instances.pas_validation' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.4'],
    entry_points = {
'console_scripts' : [
'main = pas_validation.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
