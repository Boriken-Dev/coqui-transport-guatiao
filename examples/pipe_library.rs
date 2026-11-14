//! The pipe transport as a library any guatiao host loads: a `cdylib` and nothing else, so a host written in
//! another language, or built by another compiler, offers it by its id. Its threads end before it is unloaded.

coqui_transport_guatiao::guatiao::providers!(
    id = "pipe_library",
    version = env!("CARGO_PKG_VERSION"),
    providers = [coqui_transport_guatiao::pipe::PipeProvider],
    unload = coqui_transport_guatiao::threads::done
);
