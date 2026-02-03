# Lidar-Pickler
This repository provides a pair of ROS 2 nodes for recording and replaying multi-sensor datasets. It is designed to simplify data collection for SLAM, odometry, and sensor-fusion experiments using LiDAR, IMU, camera, odometry, motion capture, and PX4 odometry streams.
# Features
LidarPickler (lidar_pickler_node.cpp):
- Records ROS 2 topics into structured files:
  - LiDAR (sensor_msgs::msg::PointCloud2) → raw .bin frames + metadata.json
  - IMU (sensor_msgs::msg::Imu) → .json logs (per-frame)
  - Camera (sensor_msgs::msg::Image) → .bmp images
  - Odometry/Pose/Mocap (nav_msgs::msg::Odometry, geometry_msgs::msg::PoseStamped) → .csv
  - PX4 VehicleOdometry (fmu in/out) (px4_msgs::msg::VehicleOdometry) → .csv
- Automatic output directory creation.
- Unified CSV schema:
  - timestamp,x,y,z,qx,qy,qz,qw
- Configurable via ROS 2 parameters (topics, directories, enabled sensors).

LidarReplay (lidar_unpickler_node.cpp)
- Reconstructs and replays previously recorded datasets into ROS 2 topics:
  - LiDAR frames from .bin + metadata
  - IMU frames from .json
- Publishes to:
  - /ouster/points (PointCloud2)
  - /ouster/imu (IMU)
- Preserves timestamps for real-time playback.
- Frame ID override support.
- Handles malformed or missing data gracefully.

# Installation
Clone and build inside your ROS 2 workspace:
```bash
cd ~/ros2_ws/src
git clone https://github.com/HydraNinja1/lidar-pickler.git
cd ~/ros2_ws
colcon build --packages-select lidar_pickler
source install/setup.bash
```
Dependencies:
- ROS 2 (tested on Humble/Foxy)
- OpenCV + cv_bridge
- nlohmann/json
- px4_msgs (for FMU odometry)

# Usage
1. Record Data
Run with desried sensor enable via parameters:
```bash
ros2 run lidar_pickler lidar_pickler_node --ros-args \
  -p lidar:=true -p imu:=true -p camera:=true \
  -p odometry:=true -p odometry_bridged:=true -p deskewed:=true -p keyframes:=true \
  -p mocap:=true -p mocap_bridged:=true -p fmu_in:=true -p fmu_out:=true \
  -p gps:=true -p common_timestamp:=true -p parent_dir:="."
```
2. Replay Data
Replays LiDAR + IMU logs in real-time:
```bash
ros2 run lidar_pickler lidar_unpickler_node --ros-args -p parent_dir:="."
```
3. Replay Fixed Rate Data
Replays LiDAR, IMU + Mocap logs in real-time:
```bash
ros2 run lidar_pickler lidar_unpickler_fixed --ros-args -p parent_dir:="."
```

