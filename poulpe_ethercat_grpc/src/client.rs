use std::{any::Any, collections::HashMap, f32::consts::E, sync::Arc, time::Duration};

use super::pb::{
    poulpe_multiplexer_client::PoulpeMultiplexerClient, PoulpeCommand, PoulpeCommands, PoulpeState,
    StateStreamRequest,
};
use crate::log_throttle::LogThrottle;
use prost_types::Timestamp;
use tokio::{
    runtime::{Builder, Handle, Runtime},
    sync::RwLock,
    time::sleep,
};
use tonic::{transport::Uri, Request};

use poulpe_ethercat_controller::{register::BoardStatus, state_machine::CiA402State};

const RECONNECT_BACKOFF_INITIAL_MS: u64 = 100;
// Cap below the 1s state-staleness window (see get_state_property). With a
// 5s ceiling, a few consecutive reconnect failures push the next attempt
// out past 1s and the cached PoulpeState in state_lock ages out before we
// can refresh it — surfacing as "state too old" in the HI even though the
// server has long since come back.
const RECONNECT_BACKOFF_MAX_MS: u64 = 300;

#[derive(Debug)]
enum Command {
    EmergencyStop(bool),
    Compliancy(bool),
    ModeOfOperation(u32),
    TargetPosition(Vec<f32>),
    TargetVelocity(Vec<f32>),
    TargetTorque(Vec<f32>),
    VelocityLimit(Vec<f32>),
    TorqueLimit(Vec<f32>),
}

#[derive(Debug)]
pub struct PoulpeRemoteClient {
    pub ids: Vec<u16>,
    pub names: Vec<String>,
    rt: Arc<Runtime>,
    addr: Uri,
    state: Arc<RwLock<HashMap<u16, PoulpeState>>>,
    command_buff: Arc<RwLock<HashMap<u16, Vec<Command>>>>,
    feedback_logs: LogThrottle,
}

impl PoulpeRemoteClient {
    /// Connect to the server with the given poulpe names
    /// - connects to the server and reads the ids of the poulpe slaves
    /// - checks if the given poulpe names are valid
    /// - subsrtibes to the states of the given slaves
    ///
    /// # Arguments
    ///     - addr: Uri: The address of the server
    ///     - poulpe_names: Vec<String>: The names of the poulpe slaves to connect to
    ///     - update_period: Duration: The update period for the state stream
    /// # Returns
    ///     - Result<Self, std::io::Error>: The client if successful, else an error
    pub fn connect_with_name(
        addr: Uri,
        poulpe_names: Vec<String>,
        update_period: Duration,
    ) -> Result<Self, std::io::Error> {
        // read all slave ids and names in the network
        let id_client = PoulpeIdClient::new(addr.clone());
        let (all_ids, all_names) = match id_client.get_slaves() {
            Ok((ids, names)) => (ids, names),
            Err(e) => {
                log::error!("Failed to connect to the server: {}", e);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Error in connecting to the server! Check if server is up!!!",
                ));
            }
        };

        // verify the names
        let mut poulpe_ids = vec![];
        for name in poulpe_names {
            // get index of the name in the all_names
            let id = all_names.iter().position(|n| n == &name);
            match id {
                Some(id) => poulpe_ids.push(all_ids[id]),
                None => {
                    log::error!("Invalid poulpe name: {}", name);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Invalid poulpe name",
                    ));
                }
            }
        }

        // create the client
        PoulpeRemoteClient::connect(addr, poulpe_ids, update_period)
    }

    /// Connect to the server with the given poulpe ids
    /// - connects to the server and reads the ids of the poulpe slaves
    /// - checks if the given poulpe ids are valid
    /// - subsrtibes to the states of the given slaves
    ///
    /// # Arguments
    ///    - addr: Uri: The address of the server
    ///    - ids: Vec<u16>: The ids of the poulpe slaves to connect to
    ///    - update_period: Duration: The update period for the state stream
    /// # Returns
    ///    - Result<Self, std::io::Error>: The client if successful, else an error
    pub fn connect(
        addr: Uri,
        poulpe_ids: Vec<u16>,
        update_period: Duration,
    ) -> Result<Self, std::io::Error> {
        let state = Arc::new(RwLock::new(HashMap::new()));
        let state_lock = Arc::clone(&state);

        let command_buff = Arc::new(RwLock::new(HashMap::new()));
        let command_buff_lock = Arc::clone(&command_buff);

        // let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let rt = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());

        // nemes of the poulpe slaves to read the states from
        let mut names = vec![];

        // Validate poulpe_ids
        let client = PoulpeIdClient::new(addr.clone());

        let url = addr.to_string();
        let ids = poulpe_ids.clone();
        // check if the id is valid and the server is up
        // check if poulpe_ids are valid
        match client.get_slaves() {
            Ok((available_ids, available_names)) => {
                let mut common_ids = available_ids.clone();
                let mut common_names = available_names.clone();
                // remove the other ids and names
                // get indexes to retain
                let mut retain_inds = vec![];
                for id in poulpe_ids.iter() {
                    if let Some(ind) = available_ids.iter().position(|&x| x == *id) {
                        retain_inds.push(ind);
                    }
                }
                common_ids = retain_inds.iter().map(|&i| available_ids[i]).collect();
                common_names = retain_inds
                    .iter()
                    .map(|&i| available_names[i].clone())
                    .collect();
                if common_ids.len() != poulpe_ids.len() || common_names.len() != poulpe_ids.len() {
                    log::error!(
                        "Invalid poulpe_ids: {:?}, available_ids: {:?}",
                        poulpe_ids,
                        available_ids
                    );
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Invalid poulpe_ids",
                    ));
                }
                // ids are good,
                names = common_names.clone();
            }
            Err(e) => {
                log::error!(
                    "Error in connecting to the server! Check if server is up!!!\n  {:?}",
                    e
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Error in connecting to the server! Check if server is up!!!",
                ));
            }
        }

        // spawn a single thread to handle both the state stream and command stream
        // with automatic reconnection on transport / stream errors.
        let poulpe_ids_for_stream: Vec<i32> = poulpe_ids.iter().map(|&id| id as i32).collect();
        rt.spawn(async move {
            let mut backoff_ms: u64 = RECONNECT_BACKOFF_INITIAL_MS;
            loop {
                // 1) (Re)connect to the server.
                let mut client = match PoulpeMultiplexerClient::connect(url.clone()).await {
                    Ok(client) => {
                        log::info!("gRPC client connected to {}", url);
                        backoff_ms = RECONNECT_BACKOFF_INITIAL_MS;
                        client
                    }
                    Err(e) => {
                        log::warn!(
                            "gRPC client connect to {} failed ({:?}); retrying in {} ms",
                            url, e, backoff_ms
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(RECONNECT_BACKOFF_MAX_MS);
                        continue;
                    }
                };

                // 2) Open the state stream.
                let state_request = Request::new(StateStreamRequest {
                    ids: poulpe_ids_for_stream.clone(),
                    update_period: update_period.as_secs_f32(),
                });
                let mut state_stream = match client.get_states(state_request).await {
                    Ok(resp) => resp.into_inner(),
                    Err(e) => {
                        log::warn!(
                            "gRPC GetStates failed ({:?}); reconnecting in {} ms",
                            e, backoff_ms
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(RECONNECT_BACKOFF_MAX_MS);
                        continue;
                    }
                };

                // 3) Open the command stream. Clone the locks once per attempt so the
                //    async_stream! capture does not consume the outer Arcs.
                let command_buff_lock_inner = Arc::clone(&command_buff_lock);
                let state_lock_inner = Arc::clone(&state_lock);
                let command_stream_period = update_period / 2;
                let command_stream = async_stream::stream! {
                    let mut interval = tokio::time::interval(command_stream_period);
                    loop {
                        interval.tick().await;
                        let mut cmd_map = command_buff_lock_inner.write().await;
                        if let Some(commands) = extract_commands(&mut cmd_map) {
                            yield commands;
                        }
                    }
                };

                // 4) Drive both streams concurrently; whichever finishes/fails first
                //    breaks us out so we can reconnect.
                tokio::select! {
                    reason = async {
                        loop {
                            match state_stream.message().await {
                                Ok(Some(poulpe_state)) => {
                                    log::debug!("Update state with {:?}", poulpe_state);
                                    let mut state_buff = state_lock_inner.write().await;
                                    for s in poulpe_state.states {
                                        state_buff.insert(s.id as u16, s);
                                    }
                                }
                                Ok(None) => break String::from("server closed state stream"),
                                Err(e) => break format!("state stream error: {:?}", e),
                            }
                        }
                    } => {
                        log::warn!("gRPC state stream ended ({}); reconnecting in {} ms", reason, backoff_ms);
                    }

                    result = client.get_commands(Request::new(command_stream)) => {
                        match result {
                            Ok(_) => log::warn!(
                                "gRPC command stream ended cleanly; reconnecting in {} ms",
                                backoff_ms
                            ),
                            Err(e) => log::warn!(
                                "gRPC command stream error ({:?}); reconnecting in {} ms",
                                e, backoff_ms
                            ),
                        }
                    }
                }

                // Brief backoff before reconnecting so we don't hammer a server that is
                // still coming back up.
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(RECONNECT_BACKOFF_MAX_MS);
            }
        });

        Ok(PoulpeRemoteClient {
            ids,
            names,
            rt,
            addr,
            state,
            command_buff,
            feedback_logs: LogThrottle::default(),
        })
    }

    /// Get all ids and names of slaves in the network
    pub fn get_poulpe_ids_sync(
        &self,
    ) -> Result<(Vec<u16>, Vec<String>), Box<dyn std::error::Error>> {
        self.rt.block_on(async {
            let mut client = PoulpeMultiplexerClient::connect(self.addr.to_string()).await?;
            get_poulpe_ids_async(&mut client).await
        })
    }

    /// get the ids of the slaves the client is connected to
    pub fn get_poulpe_ids(&self) -> Vec<u16> {
        self.rt
            .block_on(self.state.read())
            .keys()
            .cloned()
            .collect()
    }

    /// get the state property, reading stream sent by the server
    ///  
    /// - check if the state is older than 1s
    /// - if older than 1s, log an error and return an error
    fn get_state_property<T, F>(&self, slave_id: u16, f: F, _default: T) -> Result<T, ()>
    where
        F: Fn(&PoulpeState) -> T,
    {
        let state = self.rt.block_on(self.state.read());
        let state = state.get(&slave_id).ok_or_else(|| {
            if self.feedback_logs.should_log(slave_id as u32, "missing") {
                log::warn!(
                    "No state found for slave {} (repeated warnings logged at most every 5s)",
                    slave_id
                );
            }
        })?;

        if let Some(ts) = &state.published_timestamp {
            if let Ok(systime) = std::time::SystemTime::try_from(ts.clone()) {
                // SystemTime::elapsed() returns Err if the wall clock ran backwards
                // between publish and now — treat that as zero elapsed (stream just
                // arrived), not a fatal panic.
                let age = systime.elapsed().unwrap_or_default();
                if age.as_millis() > 1000 {
                    // No fresh feedback for this slave for >1s. Surface this to the
                    // caller as an error so it can react (hold position, etc.), but do
                    // NOT terminate the host process — the reconnect loop in this
                    // client and the recovery FSM in the server are expected to bring
                    // the stream back without a manual restart.
                    if self.feedback_logs.should_log(slave_id as u32, "stale") {
                        log::warn!(
                            "State for slave {} is {} ms old; slave or server may be unavailable (repeated warnings logged at most every 5s)",
                            slave_id,
                            age.as_millis()
                        );
                    }
                    return Err(());
                }
            } else {
                log::warn!("Cannot parse the timestamp, discarding message!");
                return Err(());
            }
        } else {
            log::warn!("No timestamp found for slave {}", slave_id);
        }

        Ok(f(state))
    }

    /// Get the current position of the slave [rad]
    pub fn get_position_actual_value(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(slave_id, |state| state.actual_position.clone(), vec![])
    }

    /// Get the current velocity of the slave [rad/s]
    pub fn get_velocity_actual_value(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(slave_id, |state| state.actual_velocity.clone(), vec![])
    }

    /// Get the current torque of the slave [mA]
    pub fn get_torque_actual_value(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(slave_id, |state| state.actual_torque.clone(), vec![])
    }

    /// Check if the slave is on or off
    pub fn is_on(&self, slave_id: u16) -> Result<bool, ()> {
        self.get_state_property(slave_id, |state| state.compliant, false)
    }

    /// Get the target position of the slave [rad]
    pub fn get_target_position(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(
            slave_id,
            |state| state.requested_target_position.clone(),
            vec![],
        )
    }

    /// Get the motor temperatures of the slave [°C]
    pub fn get_motor_temperatures(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(slave_id, |state| state.motor_temperatures.clone(), vec![])
    }
    /// Get the driver board temperatures of the slave [°C]
    pub fn get_board_temperatures(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(slave_id, |state| state.board_temperatures.clone(), vec![])
    }

    /// Get the mode of operation of the slave [CiA402 State]
    /// 1: Profile Position, 3: Profile Velocity, 4: Profile Torque
    /// (see poulpe_ethercat_controller::state_machine)
    pub fn get_mode_of_operation(&self, slave_id: u16) -> Result<u32, ()> {
        self.get_state_property(slave_id, |state| state.mode_of_operation as u32, 255)
    }

    /// Get the state of the slave [BoardState] (see poulpe_ethercat_controller::register)
    pub fn get_state(&self, slave_id: u16) -> Result<u32, ()> {
        // temporaty solution trasnforming the state to board status
        match BoardStatus::from_cia402_to_board_status(
            self.get_state_property(slave_id, |state| state.state, 255)?,
            self.get_state_property(slave_id, |state| state.error_codes.clone(), vec![])?,
        ) {
            Ok(s) => Ok(s as u32),
            Err(_) => Err(()),
        }
        // self.get_state_property(slave_id, |state| state.state, 255)
    }
    /// Get the state of the slave [CiA402 State]  (see poulpe_ethercat_controller::state_machine)
    pub fn get_cia402_state(&self, slave_id: u16) -> Result<u32, ()> {
        self.get_state_property(slave_id, |state| state.state, 255)
    }

    /// Get the current compliance state of the slave (the same as is_on - deprecated)
    pub fn get_torque_state(&self, slave_id: u16) -> Result<bool, ()> {
        self.get_state_property(slave_id, |state| state.compliant, false)
    }

    /// Get the current axis sensor position [rad]
    pub fn get_axis_sensors(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(slave_id, |state| state.axis_sensors.clone(), vec![])
    }

    /// Get the axis senosr absolute zeros from the firmware [rad]
    pub fn get_axis_sensor_zeros(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(slave_id, |state| state.axis_sensor_zeros.clone(), vec![])
    }

    /// Get the motor and homing error codes (see poulpe_ethercat_controller::state_machine)
    pub fn get_error_codes(&self, slave_id: u16) -> Result<Vec<i32>, ()> {
        self.get_state_property(slave_id, |state| state.error_codes.clone(), vec![])
    }

    /// Get current velocity limits (relative from 0 to 1)
    pub fn get_velocity_limit(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(
            slave_id,
            |state| state.requested_velocity_limit.clone(),
            vec![],
        )
    }
    /// Get current torque limits (relative from 0 to 1)
    pub fn get_torque_limit(&self, slave_id: u16) -> Result<Vec<f32>, ()> {
        self.get_state_property(
            slave_id,
            |state| state.requested_torque_limit.clone(),
            vec![],
        )
    }

    /// Send the command to the server
    fn push_command(&mut self, slave_id: u16, command: Command) -> Result<(), ()> {
        self.rt
            .block_on(self.command_buff.write())
            .entry(slave_id)
            .or_insert_with(Vec::new)
            .push(command);
        Ok(())
    }

    /// Turn on the slave (compliance off)
    pub fn turn_on(&mut self, slave_id: u16) {
        self.push_command(slave_id, Command::Compliancy(false));
    }

    /// Turn off the slave (compliance on)
    pub fn turn_off(&mut self, slave_id: u16) {
        self.push_command(slave_id, Command::Compliancy(true));
    }

    /// Set the mode of operation of the slave [CiA402 compliant]
    /// 1: Profile Position, 3: Profile Velocity, 4: Profile Torque
    /// (see poulpe_ethercat_controller::state_machine)
    pub fn set_mode_of_operation(&mut self, slave_id: u16, mode: u32) {
        self.push_command(slave_id, Command::ModeOfOperation(mode));
    }

    /// Set the target position of the slave [rad]
    pub fn set_target_position(&mut self, slave_id: u16, target_position: Vec<f32>) {
        self.push_command(slave_id, Command::TargetPosition(target_position));
    }
    /// Set the target velocity of the slave [rad/s]
    pub fn set_target_velocity(&mut self, slave_id: u16, target_velocity: Vec<f32>) {
        self.push_command(slave_id, Command::TargetVelocity(target_velocity));
    }
    /// Set the target torque of the slave [mA]
    pub fn set_target_torque(&mut self, slave_id: u16, target_torque: Vec<f32>) {
        self.push_command(slave_id, Command::TargetTorque(target_torque));
    }

    /// Set the velocity limit of the slave [relative from 0 to 1]
    pub fn set_velocity_limit(&mut self, slave_id: u16, velocity_limit: Vec<f32>) {
        self.push_command(slave_id, Command::VelocityLimit(velocity_limit));
    }
    /// Set the torque limit of the slave [relative from 0 to 1]
    pub fn set_torque_limit(&mut self, slave_id: u16, torque_limit: Vec<f32>) {
        self.push_command(slave_id, Command::TorqueLimit(torque_limit));
    }
    /// Emergency stop the slave
    pub fn emergency_stop(&mut self, slave_id: u16) {
        self.push_command(slave_id, Command::EmergencyStop(true));
    }
}

/// Extract the commands from the buffer and prepare them to be sent to the server
/// timestamp is added in the process to keep track of the time the command was sent
/// and to discard the command if it is too old
fn extract_commands(buff: &mut HashMap<u16, Vec<Command>>) -> Option<PoulpeCommands> {
    if buff.is_empty() {
        return None;
    }

    let mut commands = vec![];

    for (&id, cmds) in buff.iter() {
        let mut poulpe_cmd = PoulpeCommand {
            id: id.into(),
            ..Default::default()
        };
        for cmd in cmds {
            match cmd {
                Command::EmergencyStop(stop) => poulpe_cmd.emergency_stop = Some(*stop),
                Command::Compliancy(comp) => poulpe_cmd.compliancy = Some(*comp),
                Command::ModeOfOperation(mode) => poulpe_cmd.mode_of_operation = *mode as i32,
                Command::TargetPosition(pos) => {
                    if pos.len() != 0 {
                        poulpe_cmd.target_position.extend(pos.iter().cloned());
                    }
                }
                Command::TargetVelocity(vel) => {
                    if vel.len() != 0 {
                        poulpe_cmd.target_velocity.extend(vel.iter().cloned());
                    }
                }
                Command::TargetTorque(torque) => {
                    if torque.len() != 0 {
                        poulpe_cmd.target_torque.extend(torque.iter().cloned());
                    }
                }
                Command::VelocityLimit(vel) => {
                    if vel.len() != 0 {
                        poulpe_cmd.velocity_limit.extend(vel.iter().cloned());
                    }
                }
                Command::TorqueLimit(torque) => {
                    if torque.len() != 0 {
                        poulpe_cmd.torque_limit.extend(torque.iter().cloned());
                    }
                }
            }
        }
        poulpe_cmd.published_timestamp = Some(Timestamp::from(std::time::SystemTime::now()));
        commands.push(poulpe_cmd);
    }

    buff.clear();

    Some(PoulpeCommands { commands })
}

/// Get the ids nad names of slaves in the EtherCAT network
pub async fn get_poulpe_ids_async(
    client: &mut PoulpeMultiplexerClient<tonic::transport::Channel>,
) -> Result<(Vec<u16>, Vec<String>), Box<dyn std::error::Error>> {
    let response = client.get_poulpe_ids(Request::new(())).await?;
    let response = response.into_inner();
    let ids: Vec<u16> = response.ids.into_iter().map(|id| id as u16).collect();
    let names: Vec<String> = response
        .names
        .into_iter()
        .map(|name: String| name as String)
        .collect();
    Ok((ids, names))
}

/// A simple client that connects to the server and gets the ids of the slaves
/// in the network
///
/// The idea is to avoid subscribing to the slave states of the server if one
/// just wants to check which nodes are in the network
#[derive(Debug)]
pub struct PoulpeIdClient {
    rt: Arc<Runtime>,
    addr: Uri,
}
impl PoulpeIdClient {
    pub fn new(addr: Uri) -> Self {
        let rt = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        PoulpeIdClient { rt, addr }
    }

    /// Get the ids and names of the slaves in the network
    pub fn get_slaves(&self) -> Result<(Vec<u16>, Vec<String>), Box<dyn std::error::Error>> {
        self.rt.block_on(async {
            let mut client = PoulpeMultiplexerClient::connect(self.addr.to_string()).await?;
            get_poulpe_ids_async(&mut client).await
        })
    }
}
