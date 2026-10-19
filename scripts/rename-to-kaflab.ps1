param(
    [switch]$CheckOnly
)

$ErrorActionPreference = 'Stop'
$source = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path.TrimEnd('\')
$parent = Split-Path -Path $source -Parent
$destination = Join-Path $parent 'kaflab'

if ((Split-Path -Path $source -Leaf) -ne 'Kafka Client Lab') {
    throw "源目录名称不是 Kafka Client Lab：$source"
}
if (Test-Path -LiteralPath $destination) {
    throw "目标目录已存在，未覆盖：$destination"
}
if (-not (Test-Path -LiteralPath (Join-Path $source 'frontend\package.json')) -or
    -not (Test-Path -LiteralPath (Join-Path $source 'backend\app\main.py'))) {
    throw '未找到预期项目文件，取消改名。'
}

if ($CheckOnly) {
    Write-Output "准备改名：$source -> $destination"
    exit 0
}

# Windows 不允许重命名仍被开发服务或编辑器占用的工作目录。
Set-Location -LiteralPath $parent
try {
    Rename-Item -LiteralPath $source -NewName 'kaflab' -ErrorAction Stop
} catch {
    throw "目录仍被占用。请关闭此项目的 Codex/编辑器窗口及开发服务后重试。原目录未改动。详细错误：$($_.Exception.Message)"
}

if (-not (Test-Path -LiteralPath (Join-Path $destination 'frontend\package.json'))) {
    throw '改名后未找到前端项目文件，请检查新目录。'
}

# venv 的激活脚本和 pyvenv.cfg 可能记录旧的绝对路径，按文本文件逐一修正。
$venvFiles = @(
    '.venv\pyvenv.cfg', '.venv\Scripts\activate', '.venv\Scripts\activate.bat',
    'backend\client_runners\installed\python\2.13.2\pyvenv.cfg',
    'backend\client_runners\installed\python\2.13.2\Scripts\activate',
    'backend\client_runners\installed\python\2.13.2\Scripts\activate.bat',
    'backend\client_runners\installed\python\2.16.0\pyvenv.cfg',
    'backend\client_runners\installed\python\2.16.0\Scripts\activate',
    'backend\client_runners\installed\python\2.16.0\Scripts\activate.bat'
)
foreach ($relative in $venvFiles) {
    $path = Join-Path $destination $relative
    if (-not (Test-Path -LiteralPath $path)) { continue }
    $contents = [System.IO.File]::ReadAllText($path)
    $updated = $contents.Replace($source, $destination).Replace(
        $source.Replace('\', '/'), $destination.Replace('\', '/'))
    if ($updated -ne $contents) {
        [System.IO.File]::WriteAllText($path, $updated, [System.Text.UTF8Encoding]::new($false))
    }
}

Write-Output "已改名：$destination"
Write-Output '请在 Codex 中重新打开新目录。若旧虚拟环境的 pip 启动器不可用，使用 python -m pip 或重新建立虚拟环境。'
