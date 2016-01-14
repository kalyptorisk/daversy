import setup, cx_Freeze

cx_Freeze.setup(
    executables = [cx_Freeze.Executable(script="src/daversy/dvs.py", compress=True),
                   cx_Freeze.Executable(script="src/daversy/tools/dvstool_oracle.py", compress=True)],
    options     = { 'build_exe': { 'bin_excludes': 'oci.dll', 'include_msvcr': True, 'packages': 'daversy' },
                    'bdist_msi': { 'upgrade_code': '{9E829A2E-F5D1-4f14-AE3E-CF9D3D7F60C7}', 'add_to_path': True } },
    **setup.METADATA
)
