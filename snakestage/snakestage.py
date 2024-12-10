import contextlib
from snakemake.utils import read_job_properties
import re
import subprocess
import time

import gfal2

import pmgridtools.webdav_dcache as webdav
from random import shuffle

def convert_to_surl(url):
    """
    Convert a turl/surl to a tupple of (surl,turl)
    """

    return re.sub(r".*gridftp.grid.sara.nl(:2811)?/", "srm://srm.grid.sara.nl/", url)


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

    def stage(self, gfalcontext=None):
        if gfalcontext is None:
            gfalcontext = gfal2.creat_context()
        try:
            surls = [convert_to_surl(self.path)]
            # bring_online(surl, pintime, timeout, async)
            pintime = 3600 * 3
            (status, token) = gfalcontext.bring_online(surls, pintime, pintime, True)
            while status == 0:
                status = gfalcontext.bring_online_poll(surls, token)
        except gfal2.GError as e:
            print("Could not bring the file online:")
            print("\t", e.message)
            print("\t Code", e.code)

    def online(self):
        """
        check activly if job is online
        """
        wd = webdav.WebDav()
        locality = wd.locality(self._convert_to_webdav())
        print(f"loc:{locality}")
        self.onlinestatus = locality in {"ONLINE_AND_NEARLINE", "ONLINE"}
        print(f"self online status{self.onlinestatus}")
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


class Job:
    def __init__(self, slurmid):
        self.id = slurmid
        self.jobfiles = []

    def stage(self):
        for jobfile in self.jobfiles:
            jobfile.stage()

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

        # x = [
        #     x.strip()
        #     for x in open(commandfile).readlines()
        #     if x.startswith("# properties")
        # ][0]
        # parsed = json.loads(x[15:])
        # for file in [f for f in parsed["input"] if f.startswith("gridftp")]:
        #  print(file)
        # self._addFile(file)
        with contextlib.suppress(TypeError):
            for file in {f for f in read_job_properties(commandfile)["input"]
                            if f.startswith("gridftp")}:
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
        print(f"releasing {self.id}")
        self.stage()

        cmd = f"scontrol release {self.id}".split()
        print(f"sleeping {self.size()/throtle}")
        time.sleep(self.size() / throtle)

        result = subprocess.run(cmd, stdout=subprocess.PIPE)

    def hold(self, throtle=4000 * 1 << 20):
        print(f"hold {self.id}")
        self.stage()

        cmd = f"scontrol hold {self.id}".split()

        result = subprocess.run(cmd, stdout=subprocess.PIPE)


class JobFinder:
    def __init__(self):
        self.foundjobs = set()

    def findJobs(self):

        cmd = 'squeue --me -t pd -h --format="%i|%R"'.split()
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        jobids = set()
        for l2 in [l.strip('"') for l in result.stdout.decode("utf-8").splitlines()]:
            if l2.endswith("|(JobHeldUser)"):
                slurmid = l2.split("|")[0]
                if slurmid not in self.foundjobs:
                    self.foundjobs.add(slurmid)
                    jobids.add(slurmid)
        #convert to list to and shuffle it to prevent that jobs with the same file are started next to each other: thiss should prevent overloading a pool node when there are multiple jobs with the same file and requested next to eachother
        jobids_list=list(jobids)
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
        for slurmid in slurm_ids:
            # print(f"checking {(slurmid)} job if still online")
            job = Job(slurmid)
            job.lookupFiles()
            if job.online():
                # files are repinned
                job.stage()
                self.job_last_pin[slurmid] = current_time

            else:
                print(f"holding job {slurmid}")
                job.hold()
                del self.job_last_pin[slurmid]

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
                if data2stage < max_stage_GB * 1024 * 1024 * 1024:
                    job.stage()
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


def main():

    stager = StageManager()
    jobfinder = JobFinder()

    pinwaiting = PinWaitingJobs()
    waiting = pinwaiting.findJobs()
    print(f"found {waiting} jobs waiting to execute")
    pinwaiting.pin_jobs(time_last_pin=-1)
    print("loaded")
    while True:

        for slurmid in jobfinder.findJobs():
            print(f"found {slurmid}")
            job = Job(slurmid)
            try:
                job.lookupFiles()
                if job.online():
                    job.release()
                    jobfinder.foundjobs.remove(slurmid)
                    pinwaiting.add_just_staged([slurmid])
                else:
                    # store job
                    stager.add_job(job)
            except PermissionError,ValueError as e:
                print(e)

                

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
