#include <rune/gateway/SessionAuth.h>

namespace rune::gateway {

namespace pc = rune::protocol::client;

AuthGate gateFor(const SessionAuth& a, pc::TemplateId tid, bool bootstrapOpen, bool isSelfSetPassword) noexcept
{
    switch (tid) {   // the session level: always allowed
        case pc::TemplateId::Connect:
        case pc::TemplateId::HandshakeContinue:
        case pc::TemplateId::Keepalive:
        case pc::TemplateId::Ping:
        case pc::TemplateId::Disconnect:
            return AuthGate::Allow;
        default:
            break;
    }
    if (std::holds_alternative<AuthAwaitingFirst>(a))
        return tid == pc::TemplateId::AuthClientFirst || (tid == pc::TemplateId::SecurityBootstrap && bootstrapOpen)
                   ? AuthGate::Allow : AuthGate::AnswerAuthFailedAndEnd;
    if (std::holds_alternative<AuthAwaitingFinal>(a))
        return tid == pc::TemplateId::AuthClientFinal ? AuthGate::Allow : AuthGate::AnswerAuthFailedAndEnd;
    if (std::holds_alternative<AuthPasswordChangeRequired>(a))
        return tid == pc::TemplateId::SecurityDdl && isSelfSetPassword ? AuthGate::Allow
                                                                       : AuthGate::AnswerPasswordChangeRequired;
    return AuthGate::Allow;   // AuthAuthenticated / AuthAnonymous
}

} // namespace rune::gateway
