from threading import Thread
import easywebdav as wd


class STACKThread(Thread):
    def __init__(self, download_trigger):
        Thread.__init__(self)
        self.__stack_plengkeek = None
        self.__stack_geetjev = None

        self.__download_trigger = download_trigger

    @property
    def download_trigger(self):
        return self.download_trigger

    def __connect_stack_plengkeek(self):
        self.__stack_plengkeek = wd.connect(host="plengkeek.stackstorage.com", protocol="https",
                                            verify_ssl=True, username="projectgroup", password="wearethebest")

    def __connect_stack_geetjev(self):
        self.__stack_geetjev = wd.connect(host="geetjev.stackstorage.com", protocol="https",
                                          verify_ssl=True, username="projectgroup", password="wearethebest")

    def __check_dirs(self):
        def get_dirs(content):
            return [file.name for file in content if file.name != historical_path]

        # remote file path to the historical data folder
        historical_path = "/remote.php/webdav/historicaldata/"

        # get the files and folders in the historical data folder on both STACKs
        plengkeek_content = self.__stack_plengkeek.ls(historical_path)
        geetjev_content = self.__stack_geetjev.ls(historical_path)

        # put the files and folder paths in a list
        plengkeek_dirs = get_dirs(plengkeek_content)
        geetjev_dirs = get_dirs(geetjev_content)

        # compare the files, files that are missing on geetjev.stackstorage.com need to be created
        # also remove the .zip extension so that they are already converted to 'folder names'
        dirs_to_create = [file[:-4] for file in plengkeek_dirs if file not in geetjev_dirs]

        for folder in dirs_to_create:
            self.__stack_geetjev.mkdir(folder)

    def __download_next(self):
        pass

    def test(self):
        if self.__stack_plengkeek is None:
            self.__connect_stack_plengkeek()

        if self.__stack_geetjev is None:
            self.__connect_stack_geetjev()

        self.__download_trigger.wait()

service = STACKThread()
service.test()
