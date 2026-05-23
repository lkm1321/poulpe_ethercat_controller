use crate::{SlaveOffsets, SlavePos};
use std::{io, ops::Range};

/// watchdog is added to the manufcturer specific data of the statusword
/// bits 8, 14 and 15
/// parse the 3bit watchdog counter from the statusword
///
/// # Arguments
///
/// * `statusword` - The statusword of the slave
///
/// # Returns
///
/// * `u8` - The watchdog counter
fn parse_watchdog_from_status(statusword: Vec<u8>) -> u8 {
    // bit 8
    let mut watchdog_counter = (statusword[1] & 0b0000_0001);
    // bits 14 and 15
    watchdog_counter = watchdog_counter | ((statusword[1] & 0b1100_0000) >> 5);
    // return the counter
    watchdog_counter
}

/// write the watchdog counter to the controlword
/// to the bits 11-15 which are manufacturer specific
///
/// # Arguments
///
/// * `control_word` - The controlword of the slave
/// * `watchdog_counter` - The watchdog counter to be written
///
/// # Returns
///
/// * `Vec<u8>` - The controlword with the watchdog counter written
///
fn write_watchdog_to_control(control_word: Vec<u8>, watchdog_counter: u8) -> Vec<u8> {
    let mut control_word = control_word;
    // clear the bits 11-15
    control_word[1] &= 0b0000_0111;
    // write the watchdog counter to the controlword
    control_word[1] |= watchdog_counter << 3;
    // return the controlword
    control_word
}

/// verify the watchdog of the slaves
/// verify that the slaves are still writing
/// checking if the watchdog counter is the same as the previous cycle
/// - if the counter is the same, check for how long has it been the same
///      - if it is the same for more than 1s the slave is considered not responding
/// - if the counter is different, update the timestamp
/// - write the watchdog counter to the controlword
///
/// # Arguments
///
/// * `slave_number` - The number of slaves
/// * `data` - The domain data
/// * `watchdog_timeout_ms` - The timeout for the watchdog in milliseconds
/// * `watchdog_counter` - The watchdog counter to be written
/// * `slave_watchdog_control_offsets` - The controlword offsets for each slave
/// * `slave_watchdog_status_offsets` - The statusword offsets for each slave
/// * `slave_watchdog_timestamps` - The timestamps of the last watchdog update for each slave
/// * `slave_is_watchdog_responding` - The flag to check if each slave is responding
/// * `slave_previous_watchdog_counter` - The buffer to store the previous watchdog counter for each slave
/// * `slave_name_from_id` - A function to get the slave name from the id
///
/// # Returns
///
/// * `bool` - The flag to check if all the slaves are responding
pub fn verify_watchdog(
    slave_number: u32,
    data: &mut [u8],
    watchdog_timeout_ms: u32,
    watchdog_counter: u8,
    slave_watchdog_control_offsets: &Vec<Vec<Range<usize>>>,
    slave_watchdog_status_offsets: &Vec<Vec<Range<usize>>>,
    slave_watchdog_timestamps: &mut Vec<std::time::Instant>,
    slave_is_watchdog_responding: &mut Vec<bool>,
    slave_previous_watchdog_counter: &mut Vec<u8>,
    slave_name_from_id: &impl Fn(u16) -> String,
) -> bool {
    // return if all slaves responding
    let mut all_slaves_responding = true;
    // check each slave

    for i in 0..slave_number {
        // get slave watchdog control offset
        let status_offset = slave_watchdog_status_offsets[i as usize].clone();
        // get the watchdog control data
        let status_data = status_offset
            .iter()
            .map(|range| data[range.clone()].to_vec())
            .collect::<Vec<_>>();

        // doutput the watchdog status in binary
        let counter = parse_watchdog_from_status(status_data[0].clone());
        log::trace!(
            "Slave {} ({})| Watchdog counter received : {} ({:08b}), sent: {} ({:08b})",
            i,
            slave_name_from_id(i as u16),
            counter,
            counter,
            watchdog_counter,
            watchdog_counter
        );

        // check if the watchdog counter is the same as the one in the previous cycle
        // if it is the same check for how long has it been the same
        // if it is the same for more than 1s the slave is considered not responding
        if slave_previous_watchdog_counter[i as usize] == counter {
            if slave_watchdog_timestamps[i as usize].elapsed().as_millis() as u32
                > watchdog_timeout_ms
            {
                all_slaves_responding &= false;
                slave_is_watchdog_responding[i as usize] = false;
            }
        } else {
            // if the watchdog counter is different
            // update the timestamp
            slave_watchdog_timestamps[i as usize] = std::time::Instant::now();
            slave_is_watchdog_responding[i as usize] = true;
            slave_previous_watchdog_counter[i as usize] = counter;
        }

        // write the counter to the controlword
        let control_offset = slave_watchdog_control_offsets[i as usize].clone();
        for (j, range) in control_offset.iter().enumerate() {
            let control_word = data[range.clone()].to_vec();
            data[range.clone()]
                .copy_from_slice(&write_watchdog_to_control(control_word, watchdog_counter));
        }
    }
    all_slaves_responding
}

/// initialize the watchdog settings
/// find the offsets of the controlword and statusword data in the domain data
/// initialize the timestamp, flag and buffer for the watchdog data
///
/// # Arguments
///
/// * `slave_number` - The number of slaves
/// * `offsets` - The slave offsets
/// * `get_reg_addr_ranges` - A function to get the register address ranges
///
/// # Returns
///
/// * `Vec<Vec<Range<usize>>>` - The controlword offsets
/// * `Vec<Vec<Range<usize>>>` - The statusword offsets
/// * `Vec<std::time::Instant>` - The timestamps
/// * `Vec<bool>` - The flag to check if the slave is responding
/// * `Vec<u8>` - The buffer to store the watchdog data
///
pub fn init_watchdog_settings(
    slave_number: u32,
    offsets: &SlaveOffsets,
) -> Result<
    (
        Vec<Vec<Range<usize>>>,
        Vec<Vec<Range<usize>>>,
        Vec<std::time::Instant>,
        Vec<bool>,
        Vec<u8>,
    ),
    io::Error,
> {
    // initialize the watchdog variables
    // offsets of the statusword data in the domain data
    let mut slave_watchdog_status_offsets = vec![];
    // offsets of the controlword data in the domain data
    let mut slave_watchdog_control_offsets = vec![];
    // last read timestamp of the watchdog data
    let slave_watchdog_timestamps = vec![std::time::Instant::now(); slave_number as usize];
    // flag to check if the slave is responding
    let slave_is_watchdog_responding = vec![true; slave_number as usize];
    // buffer to store the watchdog data (that are read asynchronusly from the slaves)
    let slave_previous_watchdog_counter = vec![0u8; slave_number as usize];

    // Find the watchdog offsets for each slave. controlword (0x6040) and
    // statusword (0x6041) are CiA 402 mandatory — every Poulpe board MUST
    // expose them. If a slave on the bus is missing either, the slave can't
    // be safely driven (no way to command the CiA 402 state machine), so we
    // refuse to come up rather than running blind. Previously this used
    // HashMap::index, which panicked with "no entry found for key" — now it
    // returns a descriptive io::Error naming the slave_id, the missing PDO,
    // and the PDOs the slave actually exposes (so the operator can tell
    // whether it's an ESI/firmware mismatch or a transient enumeration race).
    // The caller is expected to retry the whole `EtherCatController::open`
    // a few times to absorb genuine enumeration races; persistent failure
    // surfaces a real config bug.
    for i in 0..slave_number {
        slave_watchdog_control_offsets.push(lookup_required_pdo(offsets, i as u16, "controlword")?);
    }
    for i in 0..slave_number {
        slave_watchdog_status_offsets.push(lookup_required_pdo(offsets, i as u16, "statusword")?);
    }

    Ok((
        slave_watchdog_control_offsets,
        slave_watchdog_status_offsets,
        slave_watchdog_timestamps,
        slave_is_watchdog_responding,
        slave_previous_watchdog_counter,
    ))
}

/// Looks up the address ranges of a CiA 402 mandatory PDO on `slave_id`.
/// Returns an io::Error if the slave or register is missing — never panics.
fn lookup_required_pdo(
    offsets: &SlaveOffsets,
    slave_id: u16,
    register: &str,
) -> Result<Vec<Range<usize>>, io::Error> {
    let slave_pos = SlavePos::from(slave_id);
    let slave_map = offsets.get(&slave_pos).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "init_watchdog_settings: slave_id {} not in offsets (bus enumeration incomplete)",
                slave_id
            ),
        )
    })?;
    let entries = slave_map.get(register).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "init_watchdog_settings: slave_id {} missing CiA-402 PDO {:?}. \
                 Available PDOs on this slave: {:?}. \
                 Either the bus enumeration was incomplete (transient — retry) or \
                 the slave's firmware/ESI doesn't match the expected PDO map.",
                slave_id,
                register,
                slave_map.keys().collect::<Vec<_>>()
            ),
        )
    })?;
    Ok(entries
        .iter()
        .map(|(_idx, bit_len, offset)| {
            let bytes = (*bit_len / 8) as usize;
            offset.byte..offset.byte + bytes
        })
        .collect())
}
