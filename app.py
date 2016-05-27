# Enera System Monitor
from modules.loader import load_class
import getopt
import sys


def main(argv):
    action = ''
    option = ''
    try:
        opts, args = getopt.getopt(argv, "ha:o:", ["action=", "options="])
    except getopt.GetoptError:
        print('app.py -a <action> -o <options>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('app.py -a <action>')
            sys.exit()
        elif opt in ("-a", "--action"):
            action = arg
        elif opt in ("-o", "--options"):
            option = arg

    print('# Enera Intelligence #')
    try:
        print('> Accion: ' + action)
        print('> Opciones: ' + (option if option else '---'))
        load_class('modules.' + action)()
    except ValueError:
        print('')
        sys.exit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
