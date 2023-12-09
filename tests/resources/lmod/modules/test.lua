
help( [[
   "Test module to test module load of lmod"
]])


project_dir = "/does/not/exist"

setenv("TESTLMODMODULE", "THIS IS A TEST")

prepend_path("PATH", pathJoin(project_dir, "bin"))


