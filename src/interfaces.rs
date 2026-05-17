use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use serde::{Deserialize, Serialize};
use crate::constants::INTERFACE_POLL_INTERVAL;

// ─── NetworkInterface ─────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkInterface {
    pub name: String,   // e.g. "eth0", "wlan0", "en0"
    pub addr: String,   // IPv4 address e.g. "192.168.1.10"
}

/// Returns all up, non-loopback IPv4 interfaces via getifaddrs.
pub fn list_network_interfaces() -> Vec<NetworkInterface> {
    let mut result = Vec::new();
    #[cfg(unix)]
    unsafe {
        let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifap) != 0 {
            return result;
        }
        let mut ifa = ifap;
        while !ifa.is_null() {
            let iface = &*ifa;
            if iface.ifa_addr.is_null() { ifa = iface.ifa_next; continue; }
            if (*iface.ifa_addr).sa_family as i32 != libc::AF_INET { ifa = iface.ifa_next; continue; }
            if iface.ifa_flags & libc::IFF_LOOPBACK as u32 != 0 { ifa = iface.ifa_next; continue; }
            if iface.ifa_flags & libc::IFF_UP as u32 == 0 { ifa = iface.ifa_next; continue; }

            let name = std::ffi::CStr::from_ptr(iface.ifa_name)
                .to_string_lossy().into_owned();
            let sin = &*(iface.ifa_addr as *const libc::sockaddr_in);
            let a = u32::from_be(sin.sin_addr.s_addr);
            let addr = format!(
                "{}.{}.{}.{}",
                (a >> 24) & 0xff, (a >> 16) & 0xff,
                (a >> 8) & 0xff, a & 0xff
            );
            result.push(NetworkInterface { name, addr });
            ifa = iface.ifa_next;
        }
        libc::freeifaddrs(ifap);
    }
    result
}

/// Polls for interface changes every INTERFACE_POLL_INTERVAL.
/// On change while connected, drops the connection so the engine reconnects.
/// Proactive alternative to relying on the 15s staleness watchdog.
pub fn spawn_interface_monitor(
    receive_reset_epoch: Arc<AtomicU64>,
    handshake_connected: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let mut last = list_network_interfaces();
        loop {
            std::thread::sleep(INTERFACE_POLL_INTERVAL);
            let current = list_network_interfaces();
            if current != last {
                tracing::info!(
                    "Network: interface change — was {:?} now {:?}",
                    last.iter().map(|i| format!("{}/{}", i.name, i.addr)).collect::<Vec<_>>(),
                    current.iter().map(|i| format!("{}/{}", i.name, i.addr)).collect::<Vec<_>>(),
                );
                if handshake_connected.load(Ordering::Relaxed) {
                    tracing::info!("Network: interface changed while connected — triggering reconnect");
                    handshake_connected.store(false, Ordering::Relaxed);
                    receive_reset_epoch.fetch_add(1, Ordering::Relaxed);
                }
                last = current;
            }
        }
    });
}
