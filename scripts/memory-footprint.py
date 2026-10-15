"""Read process counters, never process memory or credentials (macOS libproc)."""
import ctypes
import json
import sys


class Rusage(ctypes.Structure):
    # rusage_info_v0 in the installed macOS SDK sys/resource.h.
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64)
        for name in (
            "user_time", "system_time", "pkg_idle_wkups", "interrupt_wkups",
            "pageins", "wired_size", "resident_size", "phys_footprint",
            "proc_start_abstime", "proc_exit_abstime",
        )
    ]


pid = int(sys.argv[1])
if pid <= 0:
    raise ValueError("positive owned test PID required")
libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
libproc.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
libproc.proc_pid_rusage.restype = ctypes.c_int
info = Rusage()
if libproc.proc_pid_rusage(pid, 0, ctypes.byref(info)):
    raise OSError(ctypes.get_errno(), "cannot read owned process counters")
print(json.dumps({"pid": pid, "metric": "ri_phys_footprint", "physicalFootprint": info.phys_footprint,
                  "osResidentBytes": info.resident_size}))
