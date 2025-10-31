class StateMachine:
    def __init__(self, states, initial_state, input_alphabet, output_alphabet, transition_table):
        """
        Initialize the state machine.
        :param states: Set of states, e.g. ['S0', 'S1', ..., 'S19']
        :param initial_state: Initial state, e.g. 'S0'
        :param input_alphabet: Input alphabet, e.g. ['0', '1', ..., '9']
        :param output_alphabet: Output alphabet, e.g. ['0', '1', ..., '9']
        :param transition_table: State transition table in nested dictionary format
        """
        self.states = states
        self.initial_state = initial_state  # Initial state
        self.current_state = initial_state  # Current state, initialized to initial state
        self.input_alphabet = input_alphabet
        self.output_alphabet = output_alphabet
        self.transition_table = transition_table
        self.output_signal = []

    def process_input(self, input_string):
        """
        Process the input string, update state character by character and generate output signals.
        :param input_string: Input string
        """
        # Reset current state to initial state
        self.current_state = self.initial_state
        self.output_signal = []  # Clear output signal list

        for char in input_string:
            if char not in self.input_alphabet:
                raise ValueError(f"Invalid input: '{char}' is not in input alphabet {self.input_alphabet}.")
            
            # Update state and output signal
            self.update_state(char)
        
        return self.output_signal

    def update_state(self, input_char):
        """
        Update the state machine's state and output signal.
        :param input_char: Current input character
        """
        # Get new state and output signal
        new_state = self.transition_table[self.current_state][input_char]['next_state']
        output = self.transition_table[self.current_state][input_char]['output']

        # Update state and output signal
        self.current_state = new_state
        self.output_signal.append(output)

    def validate_transition(self, current_state, input_char, new_state, output_signal) -> bool:
        """
        Validate if a single state transition conforms to rules.
        :param current_state: Current state
        :param input_char: Input character
        :param new_state: New state
        :param output_signal: Output signal
        :return: Whether it matches the rules
        """
        try:
            expected_next_state = self.transition_table[current_state][input_char]['next_state']
            expected_output = self.transition_table[current_state][input_char]['output']
        except KeyError:
            return False

        return (
            new_state == expected_next_state and
            output_signal == expected_output
        )

    def print_results(self, input_string):
        """
        Print the results of state transitions and output signals.
        :param input_string: Input string
        """
        print("\nState transition process:")
        print("Current State | Input | New State | Output Signal")
        print("---------------------------------")
        current_state = self.initial_state  # Start from initial state
        for i, char in enumerate(input_string):
            next_state = self.transition_table[current_state][char]['next_state']
            output = self.transition_table[current_state][char]['output']
            print(f"{current_state:<13} | {char:<5} | {next_state:<9} | {output}")
            current_state = next_state  # Update current state

    def print_transition_table(self):
        """
        Print the complete state transition table.
        """
        print("\nComplete state transition table:")
        print("Current State | Input | New State | Output Signal")
        print("---------------------------------")
        for state in self.states:
            for input_char in self.input_alphabet:
                next_state = self.transition_table[state][input_char]['next_state']
                output = self.transition_table[state][input_char]['output']
                print(f"{state:<13} | {input_char:<5} | {next_state:<9} | {output}")


def generate_transition_table(states, input_alphabet, output_alphabet):
    """
    Dynamically generate a state transition table using deterministic rules to generate output signals.
    :param states: Set of states
    :param input_alphabet: Input alphabet
    :param output_alphabet: Output alphabet
    :return: State transition table
    """
    transition_table = {}
    for state in states:
        transition_table[state] = {}
        for input_char in input_alphabet:
            # New state: generated based on modulo operation (e.g., current state index + input value % number of states)
            current_state_index = int(state[1:])  # Extract state number (e.g. 'S5' -> 5)
            input_value = int(input_char)
            next_state_index = (current_state_index + input_value) % len(states)
            next_state = f"S{next_state_index}"
            
            # Output signal: generated based on deterministic rules
            output_index = (current_state_index + input_value) % len(output_alphabet)
            output = output_alphabet[output_index]
            
            # Add to state transition table
            transition_table[state][input_char] = {
                'next_state': next_state,
                'output': output
            }
    return transition_table


# from core.metrics.state_machine import StateMachine, generate_transition_table

if __name__ == "__main__":
    # Define set of states
    states = [f"S{i}" for i in range(3)]  # S0 to S10
    
    # Define initial state
    initial_state = 'S0'
    
    # Define input alphabet
    input_alphabet = [str(i) for i in range(3)]  # '0' to '9'
    
    # Define output alphabet
    output_alphabet = [str(i) for i in range(3)]  # '0' to '3'
    
    # Dynamically generate state transition table
    transition_table = generate_transition_table(states, input_alphabet, output_alphabet)

    # Create state machine instance
    sm = StateMachine(states, initial_state, input_alphabet, output_alphabet, transition_table)

    # Print complete state transition table
    sm.print_transition_table()

    # User specified input string length
    input_string = input(f"Please enter a string (containing only {input_alphabet}): ")

    # Process input
    output_signal = sm.process_input(input_string)

    # Print results
    print("\nOutput signal:", output_signal)
    sm.print_results(input_string)