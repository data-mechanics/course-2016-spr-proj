import getopt
import sys


def main():
    """ Parse user input and run application """
    try:
        # Get CLI options
        opts, args = getopt.getopt(sys.argv[1:], "", ['get-data', 'parse-data'])
    except getopt.GetoptError as e:
        print(str(e))
        usage()
        sys.exit(2)

    # If no options supplied, execute default ones
    if not opts:
        get_data()
        parse_data()
    else:
        commands = []
        for o, a in opts:
            commands.append(o)

        if '--get-data' in commands:
            get_data()
        if '--parse-data' in commands:
            parse_data()


def get_data():
    print('get data')


def parse_data():
    print('parse data')


def usage():
    """ Script usage """
    print('Usage: $ python main.py --get-data --parse-data')
    print('--get-data retrieves and stores the data sets')
    print('--parse-data transforms the data sets into something useful')
    print('The order of these commands does not matter')
    print('Without supplying any options, both are executed')

if __name__ == '__main__':
    main()
