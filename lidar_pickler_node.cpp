// Standard C++ utilities
#include <fstream>
#include <sstream>
#include <string>
#include <chrono>
#include <filesystem>  // C++17
#include <nlohmann/json.hpp>

// Core ROS 2 C++ client library
#include <rclcpp/rclcpp.hpp>

// OpenCV & ROS image bridging
#include <cv_bridge/cv_bridge.h>
#include <opencv2/opencv.hpp>
#include <iomanip>

// ROS message types 
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <px4_msgs/msg/vehicle_odometry.hpp>

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

class LidarPickler : public rclcpp::Node {
public:
    LidarPickler() : Node("lidar_pickler_node"){
        // Declare parameters to enable/disable recording of each sensor/topic
        enable_lidar_   = this->declare_parameter<bool>("lidar",   false);
        enable_imu_     = this->declare_parameter<bool>("imu",     false);
        enable_camera_  = this->declare_parameter<bool>("camera",  false);
        enable_odom_    = this->declare_parameter<bool>("odom",    false);
        enable_pose_    = this->declare_parameter<bool>("pose",    false);
        enable_mocap_   = this->declare_parameter<bool>("mocap",   false);
        enable_fmu_in_  = this->declare_parameter<bool>("fmu_in",  false);
        enable_fmu_out_ = this->declare_parameter<bool>("fmu_out", false);
        
        // Declare parameters for output directories
        lidar_dir_ = this->declare_parameter<std::string>("lidar_dir", "./lidar_frames_out");
        imu_dir_ = this->declare_parameter<std::string>("imu_dir", "./imu_frames_out");
        camera_dir_ = this->declare_parameter<std::string>("camera_dir", "./camera_frames_out");
        csvs_dir_ = this->declare_parameter<std::string>("csvs_dir",  "./csvs_out");

        // Declare parameters for topic names
        lidar_topic_ = this->declare_parameter<std::string>("lidar_topic", "/ouster/points"); 
        imu_topic_ = this->declare_parameter<std::string>("imu_topic", "/ouster/imu");
        camera_topic_ = this->declare_parameter<std::string>("camera_topic", "/cam_2/color/image_raw");
        odom_topic_ = this->declare_parameter<std::string>("odom_topic", "/glim_ros/odom");
        pose_topic_ = this->declare_parameter<std::string>("pose_topic", "/glim_ros/pose");
        mocap_topic_ = this->declare_parameter<std::string>("mocap_topic", "/vrpn_mocap/tallBoy/pose");
        fmu_in_topic_ = this->declare_parameter<std::string>("fmu_in_topic", "/fmu/in/vehicle_visual_odometry");
        fmu_out_topic_ = this->declare_parameter<std::string>("fmu_out_topic", "/fmu/out/vehicle_visual_odometry");

        // Prepare output directories only if the corresponding data stream is enabled
        if (enable_lidar_) prepare_output_dir(lidar_dir_);
        if (enable_imu_) prepare_output_dir(imu_dir_);
        if (enable_camera_) prepare_output_dir(camera_dir_);
        if (enable_odom_ || enable_pose_ || enable_mocap_ || enable_fmu_in_ || enable_fmu_out_) {
            prepare_output_dir(csvs_dir_);
        }
        
        // Create CSV files for pose-like streams if enabled
        if (enable_odom_)  init_csv(odom_csv_path_,  "odom_poses.csv");
        if (enable_pose_)  init_csv(pose_csv_path_,  "pose_poses.csv");
        if (enable_mocap_) init_csv(mocap_csv_path_, "mocap_poses.csv");
        if (enable_fmu_in_)  init_csv(fmu_in_csv_path_,  "fmu_in_poses.csv");
        if (enable_fmu_out_) init_csv(fmu_out_csv_path_, "fmu_out_poses.csv");

        // Define a QoS profile suitable for sensor data
        rclcpp::SensorDataQoS qos;
        
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

        if (enable_odom_){
            odom_sub_ = this->create_subscription<Odometry>(
                odom_topic_, qos, std::bind(&LidarPickler::odom_callback, this, _1)
            );  
        }

        if (enable_pose_){
            pose_sub_ = this->create_subscription<PoseStamped>(
                pose_topic_, qos, std::bind(&LidarPickler::pose_callback, this, _1)
            );
        }

        if (enable_mocap_){
            mocap_sub_ = this->create_subscription<PoseStamped>(
                mocap_topic_, qos, std::bind(&LidarPickler::mocap_callback, this, _1)
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

        // Log which streams are enabled for debugging and confirmation
        RCLCPP_INFO(this->get_logger(), "Pickler initialised. Enabled -> lidar:%d imu:%d cam:%d odom:%d pose:%d mocap:%d fmu_in:%d fmu_out:%d",
            enable_lidar_, enable_imu_, enable_camera_, enable_odom_, enable_pose_, enable_mocap_, enable_fmu_in_, enable_fmu_out_);
    }

private:
    // Callback function to record LiDAR point cloud messages into binary (.bin) files
    void lidar_callback(const PointCloud2::SharedPtr msg) {
        // Save metadata (e.g., point fields, sizes, etc.) of the first LiDAR frame to a JSON file.
        save_metadata_once(msg);
        
        auto timestamp = msg->header.stamp; // Extract timestamp (seconds + nanoseconds) from the ROS message header

        // Build a unique filename for this scan using timestamp in the format:
        // scan_<seconds>_<nanoseconds>.bin
        char filename_char[128];
        snprintf(filename_char, sizeof(filename_char), "%s/scan_%010u_%09u.bin", lidar_dir_.c_str(), timestamp.sec, timestamp.nanosec);

        // Open a binary file for writing (overwrite if it already exists)
        std::ofstream out(filename_char, std::ios::out | std::ios::binary | std::ios::trunc);
        if (out.is_open()) {
            // Write the raw point cloud data directly from ROS PointCloud2 into the file
            out.write(reinterpret_cast<const char*>(msg->data.data()), msg->data.size());
            out.close();
        } else {
            // If file couldn't be opened, log an error
            RCLCPP_ERROR(this->get_logger(), "Failed to open file: %s", filename_char);
        }
    }

    // Function to save LiDAR metadata to a JSON file (only on the first received frame)
    void save_metadata_once(const PointCloud2::SharedPtr msg) {
        static bool saved = false;  // Static flag ensures metadata is only saved once
        if (saved) return;          // If already saved, do nothing

        json j;  // Create a JSON object to hold metadata

        // Store basic PointCloud2 parameters
        j["height"]       = msg->height;        // Number of rows in the point cloud
        j["width"]        = msg->width;         // Number of points per row
        j["is_bigendian"] = msg->is_bigendian;  // Endianness of data
        j["point_step"]   = msg->point_step;    // Length of a point in bytes
        j["row_step"]     = msg->row_step;      // Length of a row in bytes
        j["is_dense"]     = msg->is_dense;      // True if no invalid (NaN) points
        j["frame_id"]     = msg->header.frame_id; // Coordinate frame of the data

        // Store details about each field in the point cloud
        for (const auto& field : msg->fields) {
            j["fields"].push_back({
                {"name", field.name},         // Field name (e.g., "x", "y", "z", "intensity")
                {"offset", field.offset},     // Byte offset of this field in a point record
                {"datatype", field.datatype}, // Type of data (e.g., float32, uint16)
                {"count", field.count}        // Number of elements (usually 1, but could be >1 for RGB)
            });
        }

        // Write the metadata JSON to a file inside the LiDAR output directory
        std::ofstream file(lidar_dir_ + "/metadata.json");
        file << j.dump(4);  // Pretty-print JSON with 4-space indentation
        file.close();

        // Mark as saved so we don’t write metadata again
        saved = true;
    }

    // Callback function to record IMU messages into JSON files
    void imu_callback(const Imu::SharedPtr msg) {
        // Extract timestamp from IMU message (seconds + nanoseconds)
        auto timestamp = msg->header.stamp;

        // Build filename using timestamp for uniqueness:
        // imu_<seconds>_<nanoseconds>.json
        char filename_char[128];
        snprintf(filename_char, sizeof(filename_char), "%s/imu_%010u_%09u.json",
                imu_dir_.c_str(), timestamp.sec, timestamp.nanosec);

        // JSON object to hold all IMU data for this frame
        json j;
        j["frame_id"]          = msg->header.frame_id;   // IMU frame reference
        j["timestamp_sec"]     = msg->header.stamp.sec;  // seconds part of timestamp
        j["timestamp_nanosec"] = msg->header.stamp.nanosec; // nanoseconds part

        // Orientation quaternion (from IMU)
        j["orientation"] = {
            {"x", msg->orientation.x},
            {"y", msg->orientation.y},
            {"z", msg->orientation.z},
            {"w", msg->orientation.w}
        };
        j["orientation_covariance"] = msg->orientation_covariance; // 3x3 covariance (flattened array)

        // Angular velocity (gyroscope) in rad/s
        j["angular_velocity"] = {
            {"x", msg->angular_velocity.x},
            {"y", msg->angular_velocity.y},
            {"z", msg->angular_velocity.z}
        };
        j["angular_velocity_covariance"] = msg->angular_velocity_covariance;

        // Linear acceleration (accelerometer) in m/s²
        j["linear_acceleration"] = {
            {"x", msg->linear_acceleration.x},
            {"y", msg->linear_acceleration.y},
            {"z", msg->linear_acceleration.z}
        };
        j["linear_acceleration_covariance"] = msg->linear_acceleration_covariance;

        // Write IMU data to JSON file
        std::ofstream file(filename_char);
        file << j.dump(4);  // pretty-print with 4-space indentation
        file.flush();
        file.close();
    }

    // Callback function to record camera images as OpenCV image files (.bmp)
    void camera_callback(const Image::SharedPtr msg) {
        try {
            // Convert incoming ROS Image message into an OpenCV cv::Mat
            cv_bridge::CvImagePtr cv_ptr = cv_bridge::toCvCopy(msg, msg->encoding);

            // Extract timestamp from image message
            auto timestamp = msg->header.stamp;

            // Build filename using timestamp: image_<sec>_<nanosec>.bmp
            char filename_char[128];
            snprintf(filename_char, sizeof(filename_char), "%s/image_%010u_%09u.bmp",
                    camera_dir_.c_str(), timestamp.sec, timestamp.nanosec);

            // Save the OpenCV matrix to disk as a bitmap image
            cv::imwrite(filename_char, cv_ptr->image);

            // Log confirmation 
            RCLCPP_INFO(this->get_logger(), "Saved image to: %s", filename_char);

        } catch (cv_bridge::Exception& e) {
            // If conversion from ROS Image to OpenCV Mat fails, log error
            RCLCPP_ERROR(this->get_logger(), "cv_bridge exception: %s", e.what());
        }
    }

    // Callback to record Odometry messages to CSV file
    void odom_callback(const Odometry::SharedPtr msg) {
        // Extract timestamp in seconds + fractional nanoseconds
        const auto& t = msg->header.stamp;
        double timestamp = static_cast<double>(t.sec) + static_cast<double>(t.nanosec) * 1e-9;

        // Open odometry CSV file in append mode
        std::ofstream csv(odom_csv_path_, std::ios::out | std::ios::app);
        if (!csv.is_open()) {
            // Log error if file can’t be opened
            RCLCPP_ERROR(this->get_logger(), "Failed to open CSV for appending: %s", odom_csv_path_.c_str());
            return;
        }

        // Write timestamp, position (x,y,z), and orientation quaternion (x,y,z,w)
        csv << std::fixed << std::setprecision(9)
            << timestamp << ","
            << msg->pose.pose.position.x << ","
            << msg->pose.pose.position.y << ","
            << msg->pose.pose.position.z << ","
            << msg->pose.pose.orientation.x << ","
            << msg->pose.pose.orientation.y << ","
            << msg->pose.pose.orientation.z << ","
            << msg->pose.pose.orientation.w << "\n";

        // Close file after writing
        csv.close();
    }


    // Callback to record PoseStamped messages to CSV file
    void pose_callback(const PoseStamped::SharedPtr msg) {
        // Extract timestamp in seconds + fractional nanoseconds
        const auto& t = msg->header.stamp;
        double timestamp = static_cast<double>(t.sec) + static_cast<double>(t.nanosec) * 1e-9;

        // Open pose CSV file in append mode
        std::ofstream csv(pose_csv_path_, std::ios::out | std::ios::app);
        if (!csv.is_open()) {
            // Log error if file can’t be opened
            RCLCPP_ERROR(this->get_logger(), "Failed to open CSV for appending: %s", pose_csv_path_.c_str());
            return;
        }

        // Write timestamp, position (x,y,z), and orientation quaternion (x,y,z,w)
        csv << std::fixed << std::setprecision(9)
            << timestamp << ","
            << msg->pose.position.x << ","
            << msg->pose.position.y << ","
            << msg->pose.position.z << ","
            << msg->pose.orientation.x << ","
            << msg->pose.orientation.y << ","
            << msg->pose.orientation.z << ","
            << msg->pose.orientation.w << "\n";

        // Close file after writing
        csv.close();
    }

    // Callback to record motion capture (MoCap) poses into a CSV file
    void mocap_callback(const PoseStamped::SharedPtr msg) {
        // Convert ROS timestamp (sec + nanosec) into a single double [seconds]
        const auto& t = msg->header.stamp;
        double timestamp = static_cast<double>(t.sec) + static_cast<double>(t.nanosec) * 1e-9;

        // Open mocap CSV file in append mode
        std::ofstream csv(mocap_csv_path_, std::ios::out | std::ios::app);
        if (!csv.is_open()) {
            // Log an error if the file can’t be opened
            RCLCPP_ERROR(this->get_logger(), "Failed to open CSV for appending: %s", mocap_csv_path_.c_str());
            return;
        }

        // Write one line per frame: timestamp, position (x,y,z), orientation quaternion (x,y,z,w)
        csv << std::fixed << std::setprecision(9)
            << timestamp               << ","
            << msg->pose.position.x    << ","
            << msg->pose.position.y    << ","
            << msg->pose.position.z    << ","
            << msg->pose.orientation.x << ","
            << msg->pose.orientation.y << ","
            << msg->pose.orientation.z << ","
            << msg->pose.orientation.w << "\n";

        // Close file after writing (flushes the buffer as well)
        csv.close();
    }

    // Callback to record incoming PX4 VehicleOdometry (FMU input) into CSV
    void fmu_in_callback(const VehicleOdometry::SharedPtr msg) {
        // PX4 timestamps are in microseconds → convert to seconds (double)
        const double timestamp = static_cast<double>(msg->timestamp) * 1e-6;

        // PX4 quaternion order is [w, x, y, z] (different from ROS convention)
        const double qw = msg->q[0];
        const double qx = msg->q[1];
        const double qy = msg->q[2];
        const double qz = msg->q[3];

        // Open FMU input CSV file in append mode
        std::ofstream csv(fmu_in_csv_path_, std::ios::out | std::ios::app);
        if (!csv.is_open()) {
            RCLCPP_ERROR(this->get_logger(), "Failed to open CSV for appending: %s", fmu_in_csv_path_.c_str());
            return;
        }

        // Write timestamp, position, and orientation quaternion
        // NOTE: Order is converted to ROS convention [x, y, z, w]
        csv << std::fixed << std::setprecision(9)
            << timestamp << ","
            << msg->position[0] << ","   // x
            << msg->position[1] << ","   // y
            << msg->position[2] << ","   // z
            << qx << "," 
            << qy << "," 
            << qz << "," 
            << qw << "\n"; 
        csv.close();
    }

    // Callback to record outgoing PX4 VehicleOdometry (FMU output) into CSV
    void fmu_out_callback(const VehicleOdometry::SharedPtr msg) {
        // Convert PX4 timestamp (microseconds → seconds)
        const double timestamp = static_cast<double>(msg->timestamp) * 1e-6;

        // Extract quaternion in PX4 order [w, x, y, z]
        const double qw = msg->q[0];
        const double qx = msg->q[1];
        const double qy = msg->q[2];
        const double qz = msg->q[3];

        // Open FMU output CSV file in append mode
        std::ofstream csv(fmu_out_csv_path_, std::ios::out | std::ios::app);
        if (!csv.is_open()) {
            RCLCPP_ERROR(this->get_logger(), "Failed to open CSV for appending: %s", fmu_out_csv_path_.c_str());
            return;
        }

        // Write timestamp, position, and quaternion [x,y,z,w] in ROS order
        csv << std::fixed << std::setprecision(9)
            << timestamp << ","
            << msg->position[0] << ","  // x
            << msg->position[1] << ","  // y
            << msg->position[2] << ","  // z
            << qx << "," 
            << qy << "," 
            << qz << "," 
            << qw << "\n"; 
        csv.close();
    }


    // Helper: Create output directory if it does not already exist
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

    // Helper: Initialize a CSV file with header row if it doesn’t exist
    void init_csv(std::string &path, const std::string &filename) {
        // Construct full path inside the csvs_dir_ directory
        path = (fs::path(csvs_dir_) / filename).string();

        // If the file doesn’t already exist, create it and write header
        if (!fs::exists(path)) {
            std::ofstream csv(path, std::ios::out | std::ios::trunc);
            if (!csv.is_open()) {
                // Error if file couldn’t be created
                RCLCPP_ERROR(this->get_logger(), "Failed to create CSV: %s", path.c_str());
                return;
            }

            // Write CSV header (standardized across odom, pose, mocap, fmu logs)
            csv << "timestamp,x,y,z,qx,qy,qz,qw\n";
            csv.close();
        }
    }

    // Subscribers
    rclcpp::Subscription<PointCloud2>::SharedPtr lidar_sub_;
    rclcpp::Subscription<Imu>::SharedPtr imu_sub_;
    rclcpp::Subscription<Image>::SharedPtr camera_sub_; 
    rclcpp::Subscription<Odometry>::SharedPtr odom_sub_; 
    rclcpp::Subscription<PoseStamped>::SharedPtr pose_sub_;
    rclcpp::Subscription<PoseStamped>::SharedPtr mocap_sub_;
    rclcpp::Subscription<VehicleOdometry>::SharedPtr fmu_in_sub_;
    rclcpp::Subscription<VehicleOdometry>::SharedPtr fmu_out_sub_;

    // feature flags
    bool enable_lidar_{false}, enable_imu_{false}, enable_camera_{false},
         enable_odom_{false}, enable_pose_{false}, enable_mocap_{false},
         enable_fmu_in_{false}, enable_fmu_out_{false};

    // directories
    std::string lidar_dir_, imu_dir_, camera_dir_, csvs_dir_;

    // topics
    std::string lidar_topic_, imu_topic_, camera_topic_, odom_topic_, pose_topic_, mocap_topic_, fmu_in_topic_, fmu_out_topic_;

    // csvs paths
    std::string odom_csv_path_, pose_csv_path_, mocap_csv_path_, fmu_in_csv_path_, fmu_out_csv_path_;
};
    
int main(int argc, char* argv[]) {
    rclcpp::init(argc, argv);
    rclcpp::spin(std::make_shared<LidarPickler>());
    rclcpp::shutdown();
    return 0;
}
