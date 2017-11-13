import sys, re, optparse


def process_args():
    first_re = re.compile(r'^\d{3}$')

    parser = optparse.OptionParser()
    parser.set_defaults(test=False, debug=False)
    parser.add_option('--develop', action='store_true', dest='debug')
    (options, args) = parser.parse_args()

    if len(args) == 1:
        if first_re.match(args[0]):
            print("Primary argument is : ", args[0])
        else:
            raise ValueError("First argument should be ...")
    elif len(args) > 1:
        raise ValueError("Too many command line arguments")

    if options.test:
        print('test flag set')
    
    if options.debug:
        print('debug flag set')
    
    del sys.argv[1:]

    return options
