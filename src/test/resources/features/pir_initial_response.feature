# PIR composes the initial ACK/NACK for one arrival and stages it into the
# OnHost response directory (onhost-resp). ACK means accepted-by-DCRE (F11);
# per-record rejections travel as REJ detail lines. A file-fatal arrival is
# NACKed outright. StagedWrite makes a re-run a restart no-op (R-05).
#
# The rejection outcomes are the ones PTV can actually emit. CIR's fixtures carry
# FAIL_ACCOUNT_NOT_FOUND and FAIL_EXCEEDS_MANDATE_CAP, and neither is reachable on
# the payments leg: an unknown account passes through to PAI, and there is no
# mandate tier at all.
@pir
Feature: PIR initial response back to the OnHost client

  Scenario: A fully accepted arrival is acknowledged with no rejections
    Given an arrival of 5 payment requests that all passed validation
    When the initial response job runs
    Then the response file acknowledges 5 of 5 transactions
    And the response file carries no rejection details

  Scenario: An arrival with failed validations is acknowledged with per-record rejections
    Given an arrival of 5 payment requests where the first records failed validation as:
      | outcome                 |
      | FAIL_ACCOUNT_NOT_ACTIVE |
      | FAIL_EXCEEDS_RF_BALANCE |
    When the initial response job runs
    Then the response file acknowledges 3 of 5 transactions
    And the response file lists the rejections:
      | sequence | outcome                 |
      | 1        | FAIL_ACCOUNT_NOT_ACTIVE |
      | 2        | FAIL_EXCEEDS_RF_BALANCE |

  Scenario: A file-fatal arrival is rejected outright with a NACK
    Given a file-fatal arrival declaring 3 payment requests with no verdicts recorded
    When the initial response job runs citing the fatal reason "V1 layout fails closed in production (A-2)"
    Then the response file is a NACK for 3 transactions citing "V1 layout fails closed in production (A-2)"

  Scenario: An arrival with no verdicts on record is rejected outright with a NACK
    Given a file-fatal arrival declaring 4 payment requests with no verdicts recorded
    When the initial response job runs
    Then the response file is a NACK for 4 transactions citing "NO_VERDICTS"

  Scenario: The staged response filename is durably recorded in the pir_response ledger
    Given an arrival of 5 payment requests that all passed validation
    When the initial response job runs
    Then the response file acknowledges 5 of 5 transactions
    And the response filename is recorded in pir_response as an ACK of 5 of 5

  Scenario: Re-running the response job never rewrites an existing response
    Given an arrival of 5 payment requests where the first records failed validation as:
      | outcome                 |
      | FAIL_ACCOUNT_NOT_ACTIVE |
    When the initial response job runs
    And the initial response job runs again for the same arrival
    Then the response file on disk is unchanged
    And the response file acknowledges 4 of 5 transactions
