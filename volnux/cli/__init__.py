from ._bootstrap import configure_settings_module

# Before anything imports the configuration: it reads the settings once.
configure_settings_module()
