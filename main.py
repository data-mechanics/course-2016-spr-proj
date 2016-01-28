import getopt
import os
import sys


def run():
    """ Load plugins and parse user options """
    plugins = load_plugins('plugins')

    try:
        # Get CLI options
        opts, args = getopt.getopt(sys.argv[1:], "", ['get-data', 'parse-data'])
    except getopt.GetoptError as e:
        print(str(e))
        usage()
        sys.exit(2)

    # If no options supplied, execute default ones
    if not opts:
        get_data(plugins)
        parse_data(plugins)
    else:
        commands = []
        for o, a in opts:
            commands.append(o)

        if '--get-data' in commands:
            get_data(plugins)
        if '--parse-data' in commands:
            parse_data(plugins)


def get_data(plugins):
    for Plugin in plugins:
        Plugin.get_data()
        pass


def parse_data(plugins):
    for Plugin in plugins:
        Plugin.parse_data()
        pass


def load_plugins(folder):
    """
    Get all plugins from folder and load them dynamically.
    :param folder: Root folder to get plugins from
    :return: List of imported plugin modules
    """
    plugin_names = get_plugin_names(folder)
    plugins = []
    for plugin_name in plugin_names:
        Plugin = load_plugin(plugin_name)
        plugins.append(Plugin)
    return plugins


def get_plugin_names(folder):
    """
    Search folder for all plugins to load. A plugin is simply a Python module adhering to an interface that is loaded
    dynamically.
    :param folder: Root folder to search for plugins
    :return: List of plugin names in the form of abc.def.module_name
    """
    plugins = []
    for dirpath, dirnames, filenames in os.walk(folder):
        for filename in filenames:
            # Ignore init file and internal Python folders such as __pycache__
            if filename != '__init__.py' and '__' not in dirpath:
                # Get list of individual folder names
                dirs = dirpath.split(os.sep)
                # Get rid of extension
                dirs.append(os.path.splitext(filename)[0])
                plugins.append('.'.join(dirs))
    return plugins


def load_plugin(plugin_name):
    """
    Import the plugin object dynamically.
    :param plugin_name: Name of the object to load, in the form of abc.def.module_name
    :return: Imported object
    """
    package_name, module_name = plugin_name.rsplit(".", 1)
    # TODO Switch to importlib
    Klass = __import__(plugin_name, fromlist=package_name)
    return Klass


def usage():
    """ Script usage """
    print('Usage: $ python main.py --get-data --parse-data')
    print('--get-data retrieves and stores the data sets')
    print('--parse-data transforms the data sets into something useful')
    print('The order of these commands does not matter')
    print('Without supplying any options, both are executed')

if __name__ == '__main__':
    run()
