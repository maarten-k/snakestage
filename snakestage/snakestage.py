import contextlib
import logging
import re
import subprocess
import time
from random import shuffle

import pmgridtools.api_dcache as dapi
import pmgridtools.webdav_dcache as webdav
from snakemake.utils import read_job_properties
from tqdm import tqdm


class JobFile:
    def __init__(self, path):
        self.path = path
        self.size = self.filesize()
        self.onlinestatus = None

    def __str__(self):
        return f"{self.path}\n{self.size}\n{self.online}"

    def filesize(self):
        wd = webdav.WebDav()
        self.size = wd.size(self._convert_to_webdav())
        return self.size

    def stage(self):

        dapi.stage(self._convert_to_pnfs())

    def online(self):
        """
        check activly if job is online
        """
        # wd = dapi.dcacheapy()
        locality = dapi.locality(self._convert_to_pnfs())
        # print(f"loc:{locality}")
        self.onlinestatus = locality in {"ONLINE_AND_NEARLINE", "ONLINE"}
        # print(f"self online status{self.onlinestatus}")
        return self.onlinestatus

    def _convert_to_webdav(self):
        """
        Convert a turl/surl to a webdav url
        """

        return re.sub(
            r".*/pnfs/grid.sara.nl/",
            "https://webdav.grid.surfsara.nl:2883/pnfs/grid.sara.nl/",
            self.path,
        )

    def _convert_to_pnfs(self):
        """
        convert a turl/surl to a webdav url
        """
        return re.sub(r".*/pnfs/grid.sara.nl/", "/pnfs/grid.sara.nl/", self.path)


class Job:
    def __init__(self, slurmid):
        self.id = slurmid
        self.jobfiles = []

    def stage(self):
        dapi.stage(self.get_all_files())

    def get_all_files(self):
        return [j._convert_to_pnfs() for j in self.jobfiles]

    def lookupFiles(self):
        cmd = f"scontrol show job  {self.id}".split()
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        extract_command_regex = "\sCommand=(/.*.(sbatch|sh))\n"
        commandscripts = re.findall(
            extract_command_regex, result.stdout.decode("utf-8")
        )
        if len(commandscripts) != 1:
            print(cmd)
            return

        commandfile = commandscripts[0][0]


        with contextlib.suppress(TypeError):
            for file in {
                f
                for f in read_job_properties(commandfile)["input"]
                if f.startswith("gridftp")
            }:
                self._addFile(file)

    def _addFile(self, file):
        self.jobfiles.append(JobFile(file))

    def data2stage(self):
        stagesize = 0
        for jobfile in self.jobfiles:
            if not jobfile.online():
                stagesize = stagesize + jobfile.size
        return stagesize

    def data2stage_passive(self):
        stagesize = 0
        for jobfile in self.jobfiles:
            if not jobfile.online:
                stagesize = stagesize + jobfile.size
        return stagesize

    def online(self):
        # for jobf in self.jobfiles:
        #    //print(jobf)
        #   //print(jobf.online())
        return all(jobf.online() for jobf in self.jobfiles)

    def size(self):
        """
        get total size of files on gridstorage
        """
        return sum(f.size for f in self.jobfiles)

    def release(self, throtle=4000 * 1 << 20):
        # print(f"releasing {self.id}")
        self.stage()

        cmd = f"scontrol release {self.id}".split()
        # print(f"sleeping {self.size()/throtle}")
        time.sleep(self.size() / throtle)

        subprocess.run(cmd, stdout=subprocess.PIPE)

    def hold(self):
        # print(f"hold {self.id}")
        self.stage()

        cmd = f"scontrol hold {self.id}".split()

        subprocess.run(cmd, stdout=subprocess.PIPE)


class JobFinder:
    def __init__(self):
        self.foundjobs = set()

    def findJobs(self):

        cmd = 'squeue --me -t pd -h --format="%i|%R"'.split()
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        jobids = set()
        for l2 in (l.strip('"') for l in result.stdout.decode("utf-8").splitlines()):
            if l2.endswith("|(JobHeldUser)"):
                slurmid = l2.split("|")[0]
                if slurmid not in self.foundjobs:
                    self.foundjobs.add(slurmid)
                    jobids.add(slurmid)
        # convert to list to and shuffle it to prevent that jobs with the same file are started next to each other: thiss should prevent overloading a pool node when there are multiple jobs with the same file and requested next to eachother
        jobids_list = list(jobids)
        shuffle(jobids_list)
        return jobids_list


class PinWaitingJobs:
    def __init__(self):
        self.job_last_pin = {}

    def findJobs(self):

        cmd = 'squeue --me -t pd -h --format="%i|%R"'.split()
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        # jobids = set()
        refresh = {}
        for l2 in [l.strip('"') for l in result.stdout.decode("utf-8").splitlines()]:
            if not l2.endswith("|(JobHeldUser)"):
                slurmid = l2.split("|")[0]
                if slurmid not in self.job_last_pin:
                    refresh[slurmid] = int(time.time())
                else:
                    refresh[slurmid] = self.job_last_pin[slurmid]

        self.job_last_pin = refresh
        return len(self.job_last_pin)

    def pin_jobs(self, time_last_pin=3600):
        current_time = int(time.time())

        slurm_ids = [
            slurmid
            for (slurmid, v) in self.job_last_pin.items()
            if current_time - v > time_last_pin
        ]
        print(f"checking {len(slurm_ids)} job if still online")
        all_files2stage = []
        for slurmid in tqdm(slurm_ids, ascii=True):
            # print(f"checking {(slurmid)} job if still online")
            job = Job(slurmid)
            job.lookupFiles()
            if job.online():
                # files are repinned
                all_files2stage.extend(job.get_all_files())
                self.job_last_pin[slurmid] = current_time

            else:
                print(f"holding job {slurmid}")
                job.hold()
                del self.job_last_pin[slurmid]

        # stage in chunks of 10 to prevent a 403 error on api
        chunk_size = 10
        for i in range(0, len(all_files2stage), chunk_size):
            chunk = all_files2stage[i : i + chunk_size]
            dapi.stage(chunk)

    def add_just_staged(self, slurm_ids):
        current_time = int(time.time())
        for slurmid in slurm_ids:
            self.job_last_pin[slurmid] = current_time


class StageManager:
    jobcatalog = {}
    staging = []

    def add_job(self, job):
        self.jobcatalog[job.id] = job

    def stage(self, max_stage_GB=200):
        # todo check amount of data already staged
        data2stage = 0

        for jobid, job in self.jobcatalog.items():
            if jobid not in self.staging:
                data2stage = data2stage + job.data2stage()
                # logging.debug(data2stage)
                if data2stage < max_stage_GB * 1024 * 1024 * 1024:
                    job.stage()
                    logging.debug(f"staging append {job.id}")
                    self.staging.append(job.id)
                else:
                    break

    def checkstaged(self):
        released = set()
        for id in self.staging:
            if self.jobcatalog[id].online():
                self.jobcatalog[id].release()
                self.staging.remove(id)
                del self.jobcatalog[id]
                released.add(id)
        return released


logging.basicConfig(level=logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


dapi = dapi.dcacheapy()


def main():

    stager = StageManager()
    jobfinder = JobFinder()

    pinwaiting = PinWaitingJobs()
    waiting = pinwaiting.findJobs()
    print(f"found {waiting} jobs waiting to execute")
    pinwaiting.pin_jobs(time_last_pin=-1)

    while True:
        this_round_released = 0
        for slurmid in tqdm(jobfinder.findJobs(), ascii=True):
            # print(f"found {slurmid}")
            job = Job(slurmid)
            try:
                job.lookupFiles()
                if job.online():
                    job.release()
                    jobfinder.foundjobs.remove(slurmid)
                    pinwaiting.add_just_staged([slurmid])
                    this_round_released = this_round_released + 1
                else:
                    # store job
                    stager.add_job(job)
            except (PermissionError, ValueError) as e:
                print(e)
        print(f"released {this_round_released} jobs to be executed by slurm")
        released_ids = stager.checkstaged()
        pinwaiting.add_just_staged(released_ids)
        [jobfinder.foundjobs.remove(slurmid) for slurmid in released_ids]
        pinwaiting.findJobs()
        pinwaiting.pin_jobs()
        stager.stage()
        sleeptime = 60
        for _ in range(sleeptime):
            time.sleep(1)


if __name__ == "__main__":
    main()
