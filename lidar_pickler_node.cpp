// Standard C++ utilities
#include <fstream>
#include <sstream>
#include <string>
#include <chrono>
#include <filesystem>  // C++17
#include <cmath>
#include <nlohmann/json.hpp>

// Core ROS 2 C++ client library
#include <rclcpp/rclcpp.hpp>

// OpenCV & ROS image bridging
#include <cv_bridge/cv_bridge.h>
#include <opencv2/opencv.hpp>
#include <iomanip>

// Thread-Safe Variables
#include <deque>
#include <mutex>
#include <thread>
#include <condition_variable>

// ROS message types 
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <px4_msgs/msg/vehicle_odometry.hpp>
#include <px4_msgs/msg/sensor_gps.hpp>
#include <builtin_interfaces/msg/time.hpp>

#include <pcl/io/pcd_io.h>
#include <pcl/point_types.h>
#include <pcl_conversions/pcl_conversions.h>

// Short aliases for commonly used namespaces/types
namespace fs = std::filesystem;
using json = nlohmann::json;
using std::placeholders::_1;

// Bring selected message types into the current scope
using sensor_msgs::msg::PointCloud2;
using sensor_msgs::msg::Imu;
using sensor_msgs::msg::Image;
using nav_msgs::msg::Odometry;
using geometry_msgs::msg::PoseStamped;
using px4_msgs::msg::VehicleOdometry;
using px4_msgs::msg::SensorGps;

struct FileTask {
    enum Type {BIN, JSON_TEXT, CSV_APPEND, RAW_IMAGE, PCD};
    Type type;
    std::string filename;
    std::vector<uint8_t> binary;
    std::string text;
    cv::Mat raw_image;
    std::string encoding;
    pcl::PointCloud<pcl::PointXYZ>::Ptr pcl_cloud; 
};

class LidarPickler : public rclcpp::Node {
public:
    LidarPickler() : Node("lidar_pickler_node"){
        // Declare parameters to enable/disable recording of each sensor/topic
        enable_lidar_   = this->declare_parameter<bool>("lidar",   false);
        enable_imu_     = this->declare_parameter<bool>("imu",     false);
        enable_camera_  = this->declare_parameter<bool>("camera",  false);
        enable_odometry_    = this->declare_parameter<bool>("odometry",    false);
        enable_odometry_bridged_    = this->declare_parameter<bool>("odometry_bridged",    false);
        enable_deskewed_ = this->declare_parameter<bool>("deskewed", false);
        enable_keyframes_ = this->declare_parameter<bool>("keyframes", false);
        enable_mocap_    = this->declare_parameter<bool>("mocap",    false);
        enable_mocap_bridged_    = this->declare_parameter<bool>("mocap_bridged",    false);
        enable_fmu_in_  = this->declare_parameter<bool>("fmu_in",  false);
        enable_fmu_out_ = this->declare_parameter<bool>("fmu_out", false); 
        enable_gps_ = this->declare_parameter<bool>("gps", false);       
        use_common_timestamp_ = this->declare_parameter<bool>("common_timestamp", true);
        
        // Declare parameters for output directories
        parent_dir_ = this->declare_parameter<std::string>("parent_dir", ".");
        lidar_dir_ = parent_dir_ + "/lidar_frames_out";
        imu_dir_ = parent_dir_ + "/imu_frames_out";
        camera_dir_ = parent_dir_ + "/camera_frames_out";
        csvs_dir_ = parent_dir_ + "/csvs_out";
        deskewed_dir_ = parent_dir_ + "/deskewed_frames_out";
        keyframes_dir_ = parent_dir_ + "/keyframes_out";

        // Declare parameters for topic names
        lidar_topic_ = this->declare_parameter<std::string>("lidar_topic", "/ouster/points"); 
        imu_topic_ = this->declare_parameter<std::string>("imu_topic", "/ouster/imu");
        camera_topic_ = this->declare_parameter<std::string>("camera_topic", "/flir_camera/image_raw");
        odometry_topic_ = this->declare_parameter<std::string>("odometry_topic", "/dlio/odom_node/odom");
        odometry_bridged_topic_ = this->declare_parameter<std::string>("odometry_bridged_topic", "/dlio/odom_node/odom_bridged");
        deskewed_topic_ = this->declare_parameter<std::string>("deskewed_topic", "dlio/odom_node/pointcloud/deskewed");
        keyframes_topic_ = this->declare_parameter<std::string>("keyframes_topic", "dlio/odom_node/pointcloud/keyframe");
        mocap_topic_ = this->declare_parameter<std::string>("mocap_topic", "/vrpn_mocap/Octopus/pose");
        mocap_bridged_topic_ = this->declare_parameter<std::string>("mocap_bridged_topic", "/vrpn_mocap/out/debug");
        fmu_in_topic_ = this->declare_parameter<std::string>("fmu_in_topic", "/fmu/in/vehicle_visual_odometry");
        fmu_out_topic_ = this->declare_parameter<std::string>("fmu_out_topic", "/fmu/out/vehicle_odometry");
        gps_topic_ = this->declare_parameter<std::string>("gps_topic", "/fmu/out/vehicle_gps_position");

        // Prepare output directories only if the corresponding data stream is enabled
        if (enable_lidar_) prepare_output_dir(lidar_dir_);
        if (enable_imu_) prepare_output_dir(imu_dir_);
        if (enable_camera_) prepare_output_dir(camera_dir_);
        if (enable_deskewed_) prepare_output_dir(deskewed_dir_);
        if (enable_keyframes_) prepare_output_dir(keyframes_dir_);
        if (enable_odometry_ || enable_odometry_bridged_ || enable_mocap_ || enable_mocap_bridged_ || enable_fmu_in_ || enable_fmu_out_ || enable_gps_) {
            prepare_output_dir(csvs_dir_);
        }
        
        // Create CSV files for pose-like streams if enabled
        if (enable_odometry_) init_csv(odometry_csv_path_, odometry_csv_stream_, "odometry_poses.csv");
        if (enable_odometry_bridged_) init_csv(odometry_bridged_csv_path_, odometry_bridged_csv_stream_, "odometry_bridged_poses.csv");
        if (enable_mocap_) init_csv(mocap_csv_path_, mocap_csv_stream_, "mocap_poses.csv");
        if (enable_mocap_bridged_) init_csv(mocap_bridged_csv_path_, mocap_bridged_csv_stream_, "mocap_bridged_poses.csv");
        if (enable_fmu_in_)  init_csv(fmu_in_csv_path_, fmu_in_csv_stream_,  "fmu_in_poses.csv");
        if (enable_fmu_out_) init_csv(fmu_out_csv_path_, fmu_out_csv_stream_, "fmu_out_poses.csv");
        if (enable_gps_) init_gps_csv(gps_csv_path_, gps_csv_stream_, "gps_poses.csv");

        // Creating a thread to write the ROS2 messages to the buffer
        writer_thread_ = std::thread(&LidarPickler::writer_loop, this);

        // Preallocate IMU JSON template (static fields)
        imu_template_ = {
            {"frame_id", ""},
            {"timestamp_sec", 0},
            {"timestamp_nanosec", 0},
            {"orientation", {
                {"x", 0}, {"y", 0}, {"z", 0}, {"w", 0}
            }},
            {"orientation_covariance", std::vector<double>(9, 0.0)},
            {"angular_velocity", {
                {"x", 0}, {"y", 0}, {"z", 0}
            }},
            {"angular_velocity_covariance", std::vector<double>(9, 0.0)},
            {"linear_acceleration", {
                {"x", 0}, {"y", 0}, {"z", 0}
            }},
            {"linear_acceleration_covariance", std::vector<double>(9, 0.0)}
        };

        // Define a QoS profile suitable for sensor data
        rclcpp::QoS qos = rclcpp::SensorDataQoS().keep_last(1);
        
        // Set up subscribers conditionally depending on which streams are enabled
        if (enable_lidar_){
            lidar_sub_ = this->create_subscription<PointCloud2>(
                lidar_topic_, qos, std::bind(&LidarPickler::lidar_callback, this, _1)
            );
        }

        if (enable_imu_){
            imu_sub_ = this->create_subscription<Imu>(
                imu_topic_, qos, std::bind(&LidarPickler::imu_callback, this, _1)
            );
        }

        if (enable_camera_){
            camera_sub_ = this->create_subscription<Image>(
                camera_topic_, qos, std::bind(&LidarPickler::camera_callback, this, _1)
            );
        }

        if (enable_odometry_){
            odometry_sub_ = this->create_subscription<Odometry>(
                odometry_topic_, qos, std::bind(&LidarPickler::odometry_callback, this, _1)
            );  
        }

        if (enable_odometry_bridged_){
            odometry_bridged_sub_ = this->create_subscription<VehicleOdometry>(
                odometry_bridged_topic_, qos, std::bind(&LidarPickler::odometry_bridged_callback, this, _1)
            );  
        }

        if (enable_deskewed_){
            deskewed_sub_ = this->create_subscription<PointCloud2>(
                deskewed_topic_, qos, std::bind(&LidarPickler::deskewed_callback, this, _1)
            );
        }

        if (enable_keyframes_){
            keyframes_sub_ = this->create_subscription<PointCloud2>(
                keyframes_topic_, qos, std::bind(&LidarPickler::keyframes_callback, this, _1)
            );
        }

        if (enable_mocap_){
            mocap_sub_ = this->create_subscription<PoseStamped>(
                mocap_topic_, qos, std::bind(&LidarPickler::mocap_callback, this, _1)
            );
        }

        if (enable_mocap_bridged_){
            mocap_bridged_sub_ = this->create_subscription<VehicleOdometry>(
                mocap_bridged_topic_, qos, std::bind(&LidarPickler::mocap_bridged_callback, this, _1)
            );
        }

        if (enable_fmu_in_){
            fmu_in_sub_ = this->create_subscription<VehicleOdometry>(
                fmu_in_topic_, qos, std::bind(&LidarPickler::fmu_in_callback, this, _1)
            );
        }

        if (enable_fmu_out_){
            fmu_out_sub_ = this->create_subscription<VehicleOdometry>(
                fmu_out_topic_, qos, std::bind(&LidarPickler::fmu_out_callback, this, _1)
            );
        }

        if (enable_gps_){
            gps_sub_ = this->create_subscription<SensorGps>(
                gps_topic_, qos, std::bind(&LidarPickler::gps_callback, this, _1)
            );
        }

        // Use steady (monotonic) time for a consistent, drift-free common clock
        steady_clock_ = std::make_shared<rclcpp::Clock>(RCL_STEADY_TIME);
        t0_ = steady_clock_->now();

        // Log which streams are enabled for debugging and confirmation
        RCLCPP_INFO(this->get_logger(), "Pickler initialised. Enabled -> lidar:%d imu:%d cam:%d odometry:%d odometry_bridged:%d deskewed:%d keyframes:%d mocap:%d mocap_bridged:%d fmu_in:%d fmu_out:%d gps:%d",
            enable_lidar_, enable_imu_, enable_camera_, enable_odometry_, enable_odometry_bridged_, enable_deskewed_, enable_keyframes_, enable_mocap_, enable_mocap_bridged_, enable_fmu_in_, enable_fmu_out_, enable_gps_);
    }

    
    ~LidarPickler()
    {
        // Graceful shutdown
        {
            std::lock_guard<std::mutex> lk(queue_mutex_);
            running_ = false;
        }
        queue_cv_.notify_all();
        if (writer_thread_.joinable())
            writer_thread_.join();

        if (odometry_csv_stream_.is_open())
            odometry_csv_stream_.close();

        if (odometry_bridged_csv_stream_.is_open())
            odometry_bridged_csv_stream_.close();

        if (mocap_csv_stream_.is_open())
            mocap_csv_stream_.close();

        if (mocap_bridged_csv_stream_.is_open())
            mocap_bridged_csv_stream_.close();

        if (fmu_in_csv_stream_.is_open())
            fmu_in_csv_stream_.close();

        if (fmu_out_csv_stream_.is_open())
            fmu_out_csv_stream_.close();

        if (gps_csv_stream_.is_open())
            gps_csv_stream_.close();
    }

private:
    void lidar_callback(const PointCloud2::SharedPtr msg) {
        static std::atomic<bool> saved{false};

        // Save metadata only once
        if (!saved.load()) {
            json j;
            j["height"]       = msg->height;
            j["width"]        = msg->width;
            j["is_bigendian"] = msg->is_bigendian;
            j["point_step"]   = msg->point_step;
            j["row_step"]     = msg->row_step;
            j["is_dense"]     = msg->is_dense;
            j["frame_id"]     = msg->header.frame_id;

            for (const auto& field : msg->fields) {
                j["fields"].push_back({
                    {"name", field.name},
                    {"offset", field.offset},
                    {"datatype", field.datatype},
                    {"count", field.count}
                });
            }

            std::ofstream file(lidar_dir_ + "/metadata.json");
            file << j.dump(4);
            file.close();

            saved = true;  // mark metadata as saved
        }

        // Save the binary point cloud data for this frame
        double ts = get_timestamp(&msg->header.stamp);
        uint32_t sec, nsec;
        split_time(ts, sec, nsec);

        std::array<char, 256> filename{};
        snprintf(filename.data(), sizeof(filename), "%s/scan_%010u_%09u.bin", lidar_dir_.c_str(), sec, nsec);

        FileTask t;
        t.type = FileTask::BIN;
        t.filename = filename.data();
        t.binary.resize(msg->data.size());
        memcpy(t.binary.data(), msg->data.data(), msg->data.size());

        push_task(std::move(t));
    }

    // Callback function to record IMU messages into JSON files
    void imu_callback(const Imu::SharedPtr msg) {
        double ts = get_timestamp(&msg->header.stamp);
        uint32_t sec, nsec;
        split_time(ts, sec, nsec);

        std::array<char, 256> filename{};
        snprintf(filename.data(), sizeof(filename),
                "%s/imu_%010u_%09u.json", imu_dir_.c_str(), sec, nsec);

        // Update simple fields
        imu_template_["frame_id"]          = msg->header.frame_id;
        imu_template_["timestamp_sec"]     = sec;
        imu_template_["timestamp_nanosec"] = nsec;

        // Orientation
        imu_template_["orientation"]["x"] = msg->orientation.x;
        imu_template_["orientation"]["y"] = msg->orientation.y;
        imu_template_["orientation"]["z"] = msg->orientation.z;
        imu_template_["orientation"]["w"] = msg->orientation.w;

        // Orientation covariance (FULLY optimized)
        auto &oc = imu_template_["orientation_covariance"];
        for (int i = 0; i < 9; i++)
            oc[i] = msg->orientation_covariance[i];

        // Angular velocity
        imu_template_["angular_velocity"]["x"] = msg->angular_velocity.x;
        imu_template_["angular_velocity"]["y"] = msg->angular_velocity.y;
        imu_template_["angular_velocity"]["z"] = msg->angular_velocity.z;

        // Angular velocity covariance
        auto &avc = imu_template_["angular_velocity_covariance"];
        for (int i = 0; i < 9; i++)
            avc[i] = msg->angular_velocity_covariance[i];

        // Linear acceleration
        imu_template_["linear_acceleration"]["x"] = msg->linear_acceleration.x;
        imu_template_["linear_acceleration"]["y"] = msg->linear_acceleration.y;
        imu_template_["linear_acceleration"]["z"] = msg->linear_acceleration.z;

        // Linear acceleration covariance
        auto &lac = imu_template_["linear_acceleration_covariance"];
        for (int i = 0; i < 9; i++)
            lac[i] = msg->linear_acceleration_covariance[i];

        // Create JSON write task
        FileTask t;
        t.type = FileTask::JSON_TEXT;
        t.filename = filename.data();
        t.text = imu_template_.dump() + "\n";

        push_task(std::move(t));
    }

    // Callback function to record image messages into BMP files
    void camera_callback(const Image::SharedPtr msg) {
        double ts = get_timestamp(&msg->header.stamp);
        uint32_t sec, nsec;
        split_time(ts, sec, nsec);

        std::array<char, 256> filename{};
        snprintf(filename.data(), sizeof(filename), "%s/image_%010u_%09u.bmp", camera_dir_.c_str(), sec, nsec);

        // Convert to cv::Mat
        cv_bridge::CvImagePtr cv_ptr = cv_bridge::toCvCopy(msg, msg->encoding);

        FileTask t;
        t.type = FileTask::RAW_IMAGE;
        t.filename = filename.data();
        t.raw_image = cv_ptr->image.clone();   // deep copy
        t.encoding = msg->encoding;

        push_task(std::move(t));
    }

    // Callback to record Odometry messages to CSV file
    void odometry_callback(const Odometry::SharedPtr msg) {
        double ts = get_timestamp(&msg->header.stamp);

        const auto &p = msg->pose.pose.position;
        const auto &q = msg->pose.pose.orientation;

        // Write timestamp, position (x,y,z), and orientation quaternion (x,y,z,w)
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(9)
            << ts << ","
            << p.x << "," 
            << p.y << "," 
            << p.z << ","
            << q.x << "," 
            << q.y << "," 
            << q.z << "," 
            << q.w << "\n";

        FileTask t;
        t.type = FileTask::CSV_APPEND;
        t.text = ss.str();

        t.filename = "ODOMETRY";
        push_task(std::move(t));
    }

    void odometry_bridged_callback(const VehicleOdometry::SharedPtr msg) {
        double ts = get_timestamp(&msg->timestamp);

        // PX4 quaternion order is [w, x, y, z] (different from ROS convention)
        const double qw = msg->q[0];
        const double qx = msg->q[1];
        const double qy = msg->q[2];
        const double qz = msg->q[3];

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(9)
            << ts << ","
            << msg->position[0] << ","
            << msg->position[1] << ","
            << msg->position[2] << ","
            << qx << "," 
            << qy << "," 
            << qz << "," 
            << qw << "\n"; 

        FileTask t;
        t.type = FileTask::CSV_APPEND;
        t.text = ss.str();

        // Identify which CSV stream to use
        t.filename = "ODOMETRY_BRIDGED";
        push_task(std::move(t));
    }

    void deskewed_callback(const PointCloud2::SharedPtr msg) {
        // Convert to PCL cloud
        pcl::PointCloud<pcl::PointXYZ>::Ptr cloud(new pcl::PointCloud<pcl::PointXYZ>);
        pcl::fromROSMsg(*msg, *cloud);

        // Generate timestamped filename
        double ts = get_timestamp(&msg->header.stamp);
        uint32_t sec, nsec;
        split_time(ts, sec, nsec);

        std::array<char, 256> filename{};
        snprintf(filename.data(), sizeof(filename),
                "%s/deskewed_points_%010u_%09u.pcd", deskewed_dir_.c_str(), sec, nsec);

        // Create PCD FileTask
        FileTask t;
        t.type = FileTask::PCD;
        t.filename = filename.data();
        t.pcl_cloud = cloud;

        push_task(std::move(t));
    }

    void keyframes_callback(const PointCloud2::SharedPtr msg) {
        pcl::PointCloud<pcl::PointXYZ>::Ptr cloud(new pcl::PointCloud<pcl::PointXYZ>);
        pcl::fromROSMsg(*msg, *cloud);

        double ts = get_timestamp(&msg->header.stamp);
        uint32_t sec, nsec;
        split_time(ts, sec, nsec);

        std::array<char, 256> filename{};
        snprintf(filename.data(), sizeof(filename),
                "%s/keyframe_%010u_%09u.pcd", keyframes_dir_.c_str(), sec, nsec);

        FileTask t;
        t.type = FileTask::PCD;
        t.filename = filename.data();
        t.pcl_cloud = cloud;

        push_task(std::move(t));
    }

    void mocap_callback(const PoseStamped::SharedPtr msg) {
        double ts = get_timestamp(&msg->header.stamp);

        const auto &p = msg->pose.position;
        const auto &q = msg->pose.orientation;

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(9)
            << ts << ","
            << p.x << "," 
            << p.y << "," 
            << p.z << ","
            << q.x << "," 
            << q.y << "," 
            << q.z << "," 
            << q.w << "\n";

        FileTask t;
        t.type = FileTask::CSV_APPEND;
        t.text = ss.str();
        t.filename = "MOCAP";

        push_task(std::move(t));
    }

    void mocap_bridged_callback(const VehicleOdometry::SharedPtr msg) {
        double ts = get_timestamp(&msg->timestamp);

        // PX4 quaternion order is [w, x, y, z] (different from ROS convention)
        const double qw = msg->q[0];
        const double qx = msg->q[1];
        const double qy = msg->q[2];
        const double qz = msg->q[3];

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(9)
            << ts << ","
            << msg->position[0] << ","
            << msg->position[1] << ","
            << msg->position[2] << ","
            << qx << "," 
            << qy << "," 
            << qz << "," 
            << qw << "\n"; 

        FileTask t;
        t.type = FileTask::CSV_APPEND;
        t.text = ss.str();
        t.filename = "MOCAP_BRIDGED";

        push_task(std::move(t));
    }

    // Callback to record incoming PX4 VehicleOdometry (FMU input) into CSV
    void fmu_in_callback(const VehicleOdometry::SharedPtr msg) {
        double ts = get_timestamp(&msg->timestamp);

        // PX4 quaternion order is [w, x, y, z] (different from ROS convention)
        const double qw = msg->q[0];
        const double qx = msg->q[1];
        const double qy = msg->q[2];
        const double qz = msg->q[3];

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(9)
            << ts << ","
            << msg->position[0] << ","   // x
            << msg->position[1] << ","   // y
            << msg->position[2] << ","   // z
            << qx << "," 
            << qy << "," 
            << qz << "," 
            << qw << "\n"; 
        FileTask t;

        t.type = FileTask::CSV_APPEND;
        t.text = ss.str();
        t.filename = "FMU_IN";

        push_task(std::move(t));
    }

    // Callback to record outgoing PX4 VehicleOdometry (FMU output) into CSV
    void fmu_out_callback(const VehicleOdometry::SharedPtr msg) {
        double ts = get_timestamp(&msg->timestamp);

        // PX4 quaternion order is [w, x, y, z] (different from ROS convention)
        const double qw = msg->q[0];
        const double qx = msg->q[1];
        const double qy = msg->q[2];
        const double qz = msg->q[3];

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(9)
            << ts << ","
            << msg->position[0] << ","   // x
            << msg->position[1] << ","   // y
            << msg->position[2] << ","   // z
            << qx << "," 
            << qy << "," 
            << qz << "," 
            << qw << "\n"; 
        FileTask t;

        t.type = FileTask::CSV_APPEND;
        t.text = ss.str();
        t.filename = "FMU_OUT";

        push_task(std::move(t));
    }

    // Callback to record outgoing PX4 GPS (FMU output) into CSV
    void gps_callback(const SensorGps::SharedPtr msg) {
        double timestamp = get_timestamp(&msg->timestamp);
        double timestamp_sample = get_timestamp(&msg->timestamp_sample);

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(9)
            << timestamp << ","
            << timestamp_sample << ","
            << msg->latitude_deg << "," 
            << msg->longitude_deg << "," 
            << msg->altitude_ellipsoid_m << "," 
            << msg->vel_n_m_s << ","
            << msg->vel_e_m_s << "," 
            << msg->vel_d_m_s << "," 
            << msg->fix_type << ","            
            << msg->eph << "," 
            << msg->epv << "," 
            << msg->hdop << "," 
            << msg->vdop << "," 
            << msg->s_variance_m_s << "," 
            << msg->c_variance_rad << "," 
            << msg->rtcm_msg_used << "," 
            << msg->rtcm_injection_rate << "," 
            << msg->jamming_indicator << "\n"; 
        FileTask t;

        t.type = FileTask::CSV_APPEND;
        t.text = ss.str();
        t.filename = "GPS";

        push_task(std::move(t));
    }

    void writer_loop() {
        while (true) {
            FileTask task;

            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cv_.wait(lock, [&] {
                    return !task_queue_.empty() || !running_;
                });

                if (!running_ && task_queue_.empty())
                    break;

                task = std::move(task_queue_.front());
                task_queue_.pop_front();
            }

            switch (task.type) {
                case FileTask::BIN: {
                    std::ofstream out(task.filename, std::ios::binary | std::ios::out | std::ios::trunc);
                    if (!out.is_open()) {
                        RCLCPP_ERROR(this->get_logger(),
                            "Writer thread failed to open file: %s", task.filename.c_str());
                        break;
                    }
                    out.write(reinterpret_cast<const char*>(task.binary.data()), task.binary.size());
                    break;
                }
                case FileTask::JSON_TEXT: {
                    std::ofstream out(task.filename,
                                    std::ios::out | std::ios::trunc);
                    if (!out.is_open()) {
                        RCLCPP_ERROR(this->get_logger(),
                            "Writer thread failed to open file: %s", task.filename.c_str());
                        break;
                    }
                    out << task.text;
                    break;
                }

                case FileTask::CSV_APPEND: {
                    if (task.filename == "ODOMETRY") {
                        odometry_csv_stream_ << task.text;
                    }
                    else if (task.filename == "ODOMETRY_BRIDGED") {
                        odometry_bridged_csv_stream_ << task.text;
                    }
                    else if (task.filename == "MOCAP") {
                        mocap_csv_stream_ << task.text;
                    }
                    else if (task.filename == "MOCAP_BRIDGED") {
                        mocap_bridged_csv_stream_ << task.text;
                    }
                    else if (task.filename == "FMU_IN") {
                        fmu_in_csv_stream_ << task.text;
                    }
                    else if (task.filename == "FMU_OUT") {
                        fmu_out_csv_stream_ << task.text;
                    }
                    else if (task.filename == "GPS") {
                        gps_csv_stream_ << task.text;
                    }                 
                    else {
                        RCLCPP_ERROR(this->get_logger(), "Unknown CSV stream identifier: %s", task.filename.c_str());
                    }

                    break;
                }

                case FileTask::RAW_IMAGE: {
                    // Step 1. Debayer or convert to RGB
                    cv::Mat rgb_image;

                    if (task.encoding == "bayer_rggb8" || task.encoding == "BayerRG8" || task.encoding == "bayer_rggb8") {
                        cv::cvtColor(task.raw_image, rgb_image, cv::COLOR_BayerRG2RGB);
                    } else {
                        // fallback: convert if needed
                        cv::cvtColor(task.raw_image, rgb_image, cv::COLOR_BGR2RGB);
                    }

                    // Step 2. Encode to BMP in memory
                    std::vector<uint8_t> encoded;
                    cv::imencode(".bmp", rgb_image, encoded);

                    // Step 3. Write to disk
                    std::ofstream out(task.filename, std::ios::binary | std::ios::out | std::ios::trunc);

                    if (!out.is_open()) {
                        RCLCPP_ERROR(this->get_logger(),
                                    "Writer thread failed to open file: %s",
                                    task.filename.c_str());
                        break;
                    }

                    out.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
                    break;
                }

                case FileTask::PCD: {
                    if (!task.pcl_cloud) {
                        RCLCPP_ERROR(this->get_logger(), "PCD task has no cloud: %s", task.filename.c_str());
                        break;
                    }
                    if (pcl::io::savePCDFileBinary(task.filename, *task.pcl_cloud) != 0) {
                        RCLCPP_ERROR(this->get_logger(), "Failed to save PCD file: %s", task.filename.c_str());
                    }
                    break;
                }
            }
        }
    }


    // Helper Functions:
    // Push the task in the queue
    void push_task(FileTask &&task)
    {
        size_t q_size = 0;
        {
            std::lock_guard<std::mutex> lk(queue_mutex_);
            q_size = task_queue_.size();

            if (q_size > 5000) {
                RCLCPP_WARN(this->get_logger(),
                            "Writer queue VERY LARGE (%zu). Dropped frames likely!", q_size);
            }

            task_queue_.push_back(std::move(task));
        }
        queue_cv_.notify_one();
    }

    // Create output directory if it does not already exist
    void prepare_output_dir(const std::string& dir) {
        // If directory does not exist
        if (!fs::exists(dir)) {
            // Attempt to create it (including parent dirs if needed)
            if (!fs::create_directories(dir)) {
                // Log error if creation failed
                RCLCPP_ERROR(this->get_logger(), "Failed to create output directory: %s", dir.c_str());
            } else {
                // Confirm success
                RCLCPP_INFO(this->get_logger(), "Created output directory: %s", dir.c_str());
            }
        }
    }

    // Initialize a CSV file with header row if it doesn’t exist
    void init_csv(std::string &path, std::ofstream &stream, const std::string &filename) {
        path = (fs::path(csvs_dir_) / filename).string();

        // Open persistent CSV stream (truncate old file)
        stream.open(path, std::ios::out | std::ios::trunc);

        if (!stream.is_open()) {
            RCLCPP_ERROR(this->get_logger(), "Failed to create CSV: %s", path.c_str());
            return;
        }

        // Write standardized pose header
        stream << "timestamp,x,y,z,qx,qy,qz,qw\n";
        stream.flush();
    }

    // Initialize a CSV file with header row if it doesn’t exist
    void init_gps_csv(std::string &path, std::ofstream &stream, const std::string &filename) {
        path = (fs::path(csvs_dir_) / filename).string();

        // Open persistent CSV stream (truncate old file)
        stream.open(path, std::ios::out | std::ios::trunc);

        if (!stream.is_open()) {
            RCLCPP_ERROR(this->get_logger(), "Failed to create CSV: %s", path.c_str());
            return;
        }

        // Write standardized pose header
        stream << "timestamp,timestamp_sample,latitude_deg,longitude_deg,altitude_ellipsoid_m" << ","
               << "vel_n_m_s,vel_e_m_s,vel_d_m_s" << ","
               << "fix_type,satellites_used,eph,epv,hdop,vdop" << ","
               << "s_variance_m_s,c_variance_rad,rtcm_msg_used,rtcm_injection_rate,jamming_indicator" << "\n";
        stream.flush();
    }

    // Unified timestamp getter
    double get_timestamp(const builtin_interfaces::msg::Time* header_time = nullptr) const {
        if (use_common_timestamp_ || header_time == nullptr) {
            // Seconds since node started (steady monotonic)
            return (steady_clock_->now() - t0_).seconds();
        }
        // Seconds from ROS message header
        return static_cast<double>(header_time->sec)
            + static_cast<double>(header_time->nanosec) * 1e-9;
    }

    double get_timestamp(const uint64_t* px4_timestamp_us) const {
        if (use_common_timestamp_ || px4_timestamp_us == nullptr) {
            return (steady_clock_->now() - t0_).seconds();
        }
        return static_cast<double>(*px4_timestamp_us) * 1e-6;
    }


    // split double seconds into sec/nsec for filenames
    static void split_time(double t, uint32_t &sec, uint32_t &nsec) {
        if (t < 0) t = 0;
        sec  = static_cast<uint32_t>(t);
        double frac = t - static_cast<double>(sec);
        uint64_t n = static_cast<uint64_t>(std::llround(frac * 1e9));
        if (n >= 1000000000ULL) { ++sec; n -= 1000000000ULL; }
        nsec = static_cast<uint32_t>(n);
    }

    // Subscribers
    rclcpp::Subscription<PointCloud2>::SharedPtr lidar_sub_;
    rclcpp::Subscription<Imu>::SharedPtr imu_sub_;
    rclcpp::Subscription<Image>::SharedPtr camera_sub_;
    rclcpp::Subscription<Odometry>::SharedPtr odometry_sub_;
    rclcpp::Subscription<VehicleOdometry>::SharedPtr odometry_bridged_sub_;
    rclcpp::Subscription<PointCloud2>::SharedPtr deskewed_sub_;
    rclcpp::Subscription<PointCloud2>::SharedPtr keyframes_sub_; 
    rclcpp::Subscription<PoseStamped>::SharedPtr mocap_sub_;
    rclcpp::Subscription<VehicleOdometry>::SharedPtr mocap_bridged_sub_;
    rclcpp::Subscription<VehicleOdometry>::SharedPtr fmu_in_sub_;
    rclcpp::Subscription<VehicleOdometry>::SharedPtr fmu_out_sub_; 
    rclcpp::Subscription<SensorGps>::SharedPtr gps_sub_;

    // feature flags
    bool enable_lidar_{false}, enable_imu_{false}, enable_camera_{false}, 
        enable_odometry_{false}, enable_odometry_bridged_{false}, enable_deskewed_{false}, enable_keyframes_{false},
        enable_mocap_{false}, enable_mocap_bridged_{false}, enable_fmu_in_{false}, enable_fmu_out_{false}, enable_gps_{false};

    std::shared_ptr<rclcpp::Clock> steady_clock_;
    rclcpp::Time t0_;
    bool use_common_timestamp_{true};  

    // directories
    std::string parent_dir_, lidar_dir_, imu_dir_, camera_dir_, csvs_dir_, deskewed_dir_, keyframes_dir_;

    // topics
    std::string lidar_topic_;
    std::string imu_topic_;
    std::string camera_topic_;
    std::string odometry_topic_;
    std::string odometry_bridged_topic_;
    std::string mocap_topic_;
    std::string mocap_bridged_topic_;
    std::string fmu_in_topic_; 
    std::string fmu_out_topic_;
    std::string gps_topic_;
    std::string deskewed_topic_;
    std::string keyframes_topic_;

    // csvs paths
    std::string odometry_csv_path_;
    std::string odometry_bridged_csv_path_;
    std::string mocap_csv_path_;
    std::string mocap_bridged_csv_path_;
    std::string fmu_in_csv_path_;
    std::string fmu_out_csv_path_;
    std::string gps_csv_path_;

    // persistent file stream
    std::ofstream odometry_csv_stream_;
    std::ofstream odometry_bridged_csv_stream_;
    std::ofstream mocap_csv_stream_;
    std::ofstream mocap_bridged_csv_stream_;
    std::ofstream fmu_in_csv_stream_;
    std::ofstream fmu_out_csv_stream_;
    std::ofstream gps_csv_stream_;

    // Thread-Safe Variables
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<FileTask> task_queue_;
    bool running_ = true;
    std::thread writer_thread_;

    json imu_template_;
};
    
int main(int argc, char* argv[]) {
    rclcpp::init(argc, argv);
    auto node = std::make_shared<LidarPickler>();

    // using Multi-threaded Executor allows multiple callbacks to run in parallel using separate threads
    rclcpp::executors::MultiThreadedExecutor executor;
    executor.add_node(node);
    executor.spin();
    rclcpp::shutdown();
    return 0;
}
