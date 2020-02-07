def printLogo():
    print("""    __  ___           ___________         """)
    print("""   /  |/  /___  ____ / ___/_  __/__  _____""")
    print("""  / /|_/ / __ \/ __ \\__ \ / / / _ \/ ___/""")
    print(""" / /  / / /_/ / / / /__/ // / /  __/ /    """)
    print("""/_/  /_/\____/_/ /_/____//_/  \___/_/     """)


def printHelp():
    print(
    """
    Options:
    
        --job, -j           Get Job metrics                 [boolean]
        --node, -n          Get Node metrics                [boolean]
        --statistic, -s     Get Statistic metrics           [boolean]
        --help, -h          Show help                       [boolean]
    """)                                          