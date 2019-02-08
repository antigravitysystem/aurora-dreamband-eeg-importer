from aurora import aurora
try:
    importer = aurora.SESSION_IMPORTER()
    importer.run_import()
except KeyboardInterrupt:
    print "Quit"
