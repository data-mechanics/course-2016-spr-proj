from plugins import AbstractPlugin


class MyPlugin(AbstractPlugin):
    def get_data(self):
        print('Getting data from MyPlugin')

    def parse_data(self):
        print('Parsing data from MyPlugin')
