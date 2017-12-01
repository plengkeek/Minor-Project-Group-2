from threading import Thread
import easywebdav as wd
import time


class STACKThread(Thread):
    def __init__(self, download_queue, download_trigger):
        Thread.__init__(self)
        self.__stack_plengkeek = None
        self.__stack_geetjev = None

        self._download_queue = download_queue
        self._download_trigger = download_trigger

        self._historical_path = '/remote.php/webdav/historicaldata/'

    @property
    def download_trigger(self):
        return self._download_trigger

    def _connect_stack_plengkeek(self):
        self.__stack_plengkeek = wd.connect(host="plengkeek.stackstorage.com", protocol="https",
                                            verify_ssl=True, username="projectgroup", password="wearethebest")

    def _connect_stack_geetjev(self):
        self.__stack_geetjev = wd.connect(host="geetjev.stackstorage.com", protocol="https",
                                          verify_ssl=True, username="projectgroup", password="wearethebest")

    def _get_dirs(self, connection, path):
        content = connection.ls(path)
        return [file.name for file in content if file.name != path]

    def _check_dirs(self):
        # remote file path to the historical data folder
        self._historical_path = "/remote.php/webdav/historicaldata/"

        # get the directories
        plengkeek_dirs = self._get_dirs(self.__stack_plengkeek, self._historical_path)
        geetjev_dirs = self._get_dirs(self.__stack_geetjev, self._historical_path)

        # compare the files, files that are missing on geetjev.stackstorage.com need to be created
        # also remove the .zip extension so that they are already converted to 'folder names'
        dirs_to_create = [file[:-4] for file in plengkeek_dirs if file not in geetjev_dirs]

        for folder in dirs_to_create:
            self.__stack_geetjev.mkdir(folder)

    def _download_next(self):
        file_path = self._download_queue.get()
        path_as_list = file_path.split("/")

        remote_path = "/".join(path_as_list[:-1]) + "/"
        local_file = path_as_list[-1]

        print('start download')

        remote_path = '/remote.php/webdav/'
        local_file = 'measurements_current.xml'
        self.__stack_plengkeek.download(remote_path, local_file)

        print("Done downloading")
        time.sleep(30)

    def test(self):
        if self.__stack_plengkeek is None:
            self._connect_stack_plengkeek()

        if self.__stack_geetjev is None:
            self._connect_stack_geetjev()

        # noinspection PyTypeChecker
        dirs_to_download = self._get_dirs(self.__stack_plengkeek, self._historical_path)
        self._download_queue.fill(dirs_to_download)

        #TODO: REMOVE
        self._download_next()

        if self.download_trigger.is_set() is True:
            pass

from download_queue import DownloadQueue
from threading import Event

service = STACKThread(DownloadQueue(), Event())
service.test()
